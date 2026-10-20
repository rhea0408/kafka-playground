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

Commands: `AddComponent`, `RemoveComponent`, `Connect`, `Disconnect`, `UpdateConfig`,
`ProduceManual`, `KillConsumer`, `RestartConsumer`. Events: `ComponentAdded`, `ComponentRemoved`,
`Connected`, `Disconnected`, `ConfigUpdated`, `RecordProduced`, `RecordAppended`, `ProduceAcked`,
`ProduceFailed`, `RecordConsumed`, `OffsetCommitted`, `ConsumerStopped`, `ConsumerStarted`.

`UpdateConfig { id, patch }` merges `patch` into the component's config and validates it with
`@kp/schema`. Topics can only grow (a decrease is rejected with Kafka's `InvalidPartitionsException`
wording); a producer rate change restarts its schedule from now; consumer changes apply from the
next `poll()`; ids, `group.id`, `auto.offset.reset`, the replication factor and `broker.rack` are
rejected. A patch that changes nothing is accepted without an event and is not logged. Full rules
in `src/commands/update-config.ts`.

Trace ids (`RecordProduced.traceId` and onwards) look like `p1@3#7`: producer id, the producer's
incarnation (one per life; a removed and re-added producer gets a new one) and its record count.
They are unique for the whole run, so the UI can key message dots by them.

## Module map

```
src/
├─ core/          Kafka-free simulation kernel
│  ├─ rng.ts        seeded sfc32 PRNG; fork(label) gives independent streams
│  ├─ clock.ts      integer-ms sim clock
│  ├─ queue.ts      binary heap ordered by (time, seq)
│  └─ kernel.ts     the loop: clock + queue + RNG + ordered event stream
├─ model/         data and pure functions
│  ├─ partition.ts  append-only log with logStartOffset / LEO / HW, record sizes
│  ├─ placement.ts  partition leaders across brokers
│  ├─ state.ts      engine state (brokers, topics, producers, consumers, groups, parked fetches)
│  ├─ tasks.ts      scheduled work as plain data (ProducerTick, LeaderAppend, FetchRequest, …)
│  └─ context.ts    what handlers get: kernel, state, emit, config
├─ protocols/     Kafka behaviour, each with Kafka doc / source citations
│  ├─ partitioner.ts  murmur2 (ported from the Java client) + sticky partitioner
│  ├─ produce.ts      send → leader append → ack, acks 0 / 1 / all
│  ├─ consume.ts      poll(), fetch buffer, auto.offset.reset, auto-commit, close vs crash
│  ├─ fetch.ts        broker side of the long poll (fetch.min.bytes / fetch.max.wait.ms)
│  ├─ metadata.ts     client metadata cache (metadata.max.age.ms): when clients see new partitions
│  └─ index.ts        runTask: the single task switch
├─ commands/      Command union (index.ts), handlers (apply.ts), UpdateConfig (update-config.ts)
├─ events/        Event union (index.ts) and one-line formatting (format.ts)
├─ snapshot/      getSnapshot() builder, including consumer lag
├─ testing/       fast-check generators and scenario runner (tests only)
└─ engine.ts      createEngine / replayCommandLog: wires the above together
scripts/demo.ts   headless demo (Node runs the TypeScript directly)
```

## How an event flows through the engine

One keyed record, `acks=all`, with the default 1 ms network delay and a consumer whose fetch is
already waiting at the broker (the usual case):

```
t=500  ProducerTick task        (scheduled at startedAt + floor(n × 1000 / rate))
       ├─ chooseKey → "B"        (producer's seeded RNG stream)
       ├─ partitionForKey("B", 3) → 1        toPositive(murmur2(utf8)) % partitions (as the
       │                                     producer's cached metadata knows them)
       ├─ emit RecordProduced    traceId "p1@1#1"
       └─ schedule LeaderAppend at t+1, and the next ProducerTick
t=501  LeaderAppend task
       ├─ appendRecord(orders-1) → offset 4; LEO = HW = 5 (single replica)
       ├─ emit RecordAppended
       ├─ the HW moved: a parked FetchRequest for orders-1 now has ≥ fetch.min.bytes → answer it
       └─ schedule FetchResponse, then ProduceResponse, at t+1
t=502  FetchResponse task    → records go into the consumer's fetch buffer; the application is
       │                       blocked in poll(), so poll() continues:
       ├─ auto-commit positions from earlier polls if auto.commit.interval.ms has passed
       ├─ return buffered records, at most max.poll.records; emit RecordConsumed for each
       ├─ send the next FetchRequest (pipelining: it waits at the broker while the app works)
       └─ schedule the next poll() after records × processingTimeMs
t=502  ProduceResponse task  → emit ProduceAcked { offset: 4 }
```

With no data, a FetchRequest waits at the broker for up to `fetch.max.wait.ms` (500 ms) and then
returns empty; the consumer sends the next one. While blocked in `poll()` the consumer also wakes
up when an auto-commit is due.

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
