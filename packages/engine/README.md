# @kp/engine

The Kafka Playground simulation engine: a deterministic, headless discrete-event simulation of
Kafka 4.x (KRaft) in pure TypeScript. The same code runs in the browser's Web Worker, in Node
tests and in the fidelity harness. It never touches the DOM, wall-clock time or `Math.random`
(enforced by ESLint and by a DOM-free `lib` in `tsconfig`).

```
pnpm --filter @kp/engine test        # unit + property tests
pnpm --filter @kp/engine demo        # headless event-log demo (-- --seed 7 --until 12000)
```

## Using it

```ts
import { createEngine, formatEvent } from '@kp/engine';

const engine = createEngine({
  seed: 42,
  topology: {
    brokers: [{ kind: 'broker', id: 'b1' }],
    topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
    producers: [{ kind: 'producer', id: 'p1', ratePerSec: 2 }],
    consumers: [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest' }],
    connections: [
      { from: 'p1', to: 'orders' },
      { from: 'orders', to: 'c1' },
    ],
  },
});
engine.on((event) => console.log(formatEvent(event)));
engine.runUntil(5000); // advance sim time
engine.dispatch({ type: 'KillConsumer', id: 'c1' }); // commands change state
const snapshot = engine.getSnapshot(); // plain JSON for the UI
```

| API                                                                               | What it does                                                                                         |
| --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `createEngine({ seed, topology, networkDelayMs?, snapshotRecordsPerPartition? })` | Builds an engine; the topology is validated with `@kp/schema`                                        |
| `dispatch(command)`                                                               | Validates and applies a command at the current sim time; returns `{ ok }` or `{ ok: false, reason }` |
| `step()` / `runUntil(t)`                                                          | Runs the next scheduled task / every task up to and including `t`                                    |
| `on(listener)`                                                                    | Subscribes to the event stream; returns an unsubscribe function                                      |
| `getSnapshot()`                                                                   | Plain, JSON-serialisable copy of the state (offsets, records, lag, …)                                |
| `commandLog`                                                                      | Every accepted command as `{ t, afterStep, command }`                                                |
| `replayCommandLog(options, log)`                                                  | Rebuilds the exact same run on a fresh engine                                                        |

Commands: `AddComponent`, `RemoveComponent`, `Connect`, `Disconnect`, `ProduceManual`,
`KillConsumer`, `RestartConsumer`. Events: `ComponentAdded`, `ComponentRemoved`, `Connected`,
`Disconnected`, `RecordProduced`, `RecordAppended`, `ProduceAcked`, `ProduceFailed`,
`RecordConsumed`, `OffsetCommitted`, `ConsumerStopped`, `ConsumerStarted`.

## Module map

```
src/
├─ core/          Kafka-free simulation kernel
│  ├─ rng.ts        seeded sfc32 PRNG; fork(label) gives independent streams
│  ├─ clock.ts      integer-ms sim clock
│  ├─ queue.ts      binary heap ordered by (time, seq)
│  └─ kernel.ts     the loop: clock + queue + RNG + ordered event stream
├─ model/         data and pure functions
│  ├─ partition.ts  append-only log with logStartOffset / LEO / HW
│  ├─ placement.ts  partition leaders across brokers
│  ├─ state.ts      engine state (brokers, topics, producers, consumers, groups)
│  ├─ tasks.ts      scheduled work as plain data (ProducerTick, LeaderAppend, …)
│  └─ context.ts    what handlers get: kernel, state, emit, config
├─ protocols/     Kafka behaviour, each with Kafka doc / source citations
│  ├─ partitioner.ts  murmur2 (ported from the Java client) + sticky partitioner
│  ├─ produce.ts      send → leader append → ack, acks 0 / 1 / all
│  ├─ consume.ts      poll loop, auto.offset.reset, auto-commit, close vs crash
│  └─ index.ts        runTask: the single task switch
├─ commands/      Command union (index.ts) and handlers (apply.ts)
├─ events/        Event union (index.ts) and one-line formatting (format.ts)
├─ snapshot/      getSnapshot() builder, including consumer lag
├─ testing/       fast-check generators and scenario runner (tests only)
└─ engine.ts      createEngine / replayCommandLog: wires the above together
scripts/demo.ts   headless demo (Node runs the TypeScript directly)
```

## How an event flows through the engine

One keyed record, `acks=all`, with the default 1 ms network delay:

```
t=500  ProducerTick task        (scheduled at startedAt + floor(n × 1000 / rate))
       ├─ chooseKey → "B"        (producer's seeded RNG stream)
       ├─ partitionForKey("B", 3) → 1        toPositive(murmur2(utf8)) % partitions
       ├─ emit RecordProduced
       └─ schedule LeaderAppend at t+1, and the next ProducerTick
t=501  LeaderAppend task
       ├─ appendRecord(orders-1) → offset 4; LEO = HW = 5 (single replica)
       ├─ emit RecordAppended
       └─ schedule ProduceResponse at t+1
t=502  ProduceResponse task  → emit ProduceAcked { offset: 4 }
t=600  ConsumerPoll task
       ├─ auto-commit positions from earlier polls if auto.commit.interval.ms has passed
       ├─ read below the HW from each assigned partition, at most max.poll.records in total
       ├─ emit RecordConsumed for each record; position = offset + 1
       └─ schedule the next poll after records × processingTimeMs (or pollIntervalMs if empty)
```

Every arrow is a task in the `(time, seq)` queue, and every effect the outside world can see is an
event. The UI sends commands, renders events and snapshots, and never mutates engine state.

## Determinism

Same seed + same topology + same command log ⇒ byte-identical event log (NFR-4.1).

- All randomness comes from `createRng(seed)` and its `fork(label)` streams, using integer maths only.
- Time is the integer sim clock. Ties at one millisecond are broken by `seq` (scheduling order).
- State lives in insertion-ordered `Map`s and arrays. Iteration order follows the command log.
- Commands are logged with `afterStep`, the number of tasks run before them, so a command sent
  between two tasks of the same millisecond replays in exactly the same place.
- `determinism.test.ts` and the fast-check properties in `invariants.prop.test.ts` check this on
  every CI run.

Where the simulation differs from real Kafka is listed in `docs/simplifications.md`.
