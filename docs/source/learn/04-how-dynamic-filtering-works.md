# How Dynamic Filtering Works

DataFusion can push a runtime predicate from a pipeline-breaking operator into
a `DataSourceExec`. The predicate is a `DynamicFilterPhysicalExpr`: it starts
as `true` (printed as `DynamicFilter [ empty ]`) and is overwritten as the
producer runs. The scan uses the current value to skip files, row groups, and
rows.

Typical producers:

- `HashJoinExec` — after the build side is in memory, push a range and/or
  IN-list of join keys into the probe scan.
- `SortExec` (TopK) — as the heap fills, push a bound so later rows that cannot
  enter the top-K are skipped.
- `AggregateExec` — similar min/max bounds for some aggregates.

A FULL join never carries a dynamic filter. Probe rows that fail the predicate
would still have to be emitted as unmatched.

This is ordinary DataFusion behavior. Distributed planning does not add a
separate API for it. What changes is whether the producer and the scan still
share one `DynamicFilterPhysicalExpr` after the plan is split into stages.

See the
[DataFusion dynamic filters blog](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/)
for the single-node design.

## Local: producer and scan in the same stage

When both nodes live in one stage, they share one in-memory filter. Sending
that stage to a worker keeps them linked, so DataFusion's existing pushdown
runs on the worker.

A broadcast inner join is the usual example: only the build side crosses a
network boundary. The join and the probe scan stay together:

```text
┌───── Stage 2 ── tasks=3 ─────────────────────────────┐
│ HashJoinExec: mode=CollectLeft                       │  ← producer
│   CoalescePartitionsExec                             │
│     [Stage 1] NetworkBroadcastExec                   │  ← build side
│   DistributedLeafExec:                               │
│     DataSourceExec: predicate=DynamicFilter [ empty ]│  ← probe, same stage
└──────────────────────────────────────────────────────┘
```

A TopK sort sitting on its scan is the same layout:

```text
┌───── Stage 1 ────────────────────────────────────────┐
│ SortExec: TopK(fetch=5)                              │  ← producer
│   DistributedLeafExec:                               │
│     DataSourceExec: predicate=DynamicFilter [ empty ]│  ← same stage
└──────────────────────────────────────────────────────┘
```

Each task has its own copy of the filter and updates only that copy. After a
CollectLeft join executes in-process, display can show the filled-in
predicate, for example:

```text
DataSourceExec: predicate=DynamicFilter [ id@0 >= 0 AND id@0 <= 99 AND id@0 IN (SET) ([...]) ]
```

`dynamic_rg_pruning=eligible` on a scan means that scan is wired to re-evaluate
row-group statistics when the filter is updated.

## Remote: producer and scan in different stages

A partitioned hash join shuffles both sides, so the join and the probe scan
are encoded as separate plans:

```text
┌───── Stage 3 ────────────────────────────────────────┐
│ HashJoinExec: mode=Partitioned                       │  ← producer
│   [Stage 1] NetworkShuffleExec                       │
│   [Stage 2] NetworkShuffleExec                       │
└──────────────────────────────────────────────────────┘
┌───── Stage 2 ────────────────────────────────────────┐
│ RepartitionExec                                      │
│   DistributedLeafExec:                               │
│     DataSourceExec: predicate=DynamicFilter [ empty ]│  ← different plan
└──────────────────────────────────────────────────────┘
```

The scan's filter is a different instance from the join's. Build-side updates
never reach it. After execution the coordinator still prints
`DynamicFilter [ empty ]` on that scan.

## CollectLeft rewrite

`normalize_collect_joins` rewrites some CollectLeft joins to
`PartitionMode::Partitioned` so they can run in a multi-task stage. The
rewrite keeps the original `DynamicFilterPhysicalExpr` — the same instance
the probe scan already subscribed to — rather than allocating a replacement.

That rewrite does not make the filter cross a network boundary. Once the
partitioned join is placed above `NetworkShuffleExec`s, the producer and scan
are in the remote case above.

## What EXPLAIN shows

`predicate=DynamicFilter [ empty ]` is the planned predicate on the
coordinator's copy of the plan:

- In the local case it is the starting value. The worker may have updated its
  own copy during execution; that update is not written back into the
  coordinator plan.
- In the remote case it is also the value the scan actually used. Filter
  updates do not cross network boundaries.
