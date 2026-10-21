import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine, type Engine } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';

function record(engine: Engine): EngineEvent[] {
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  return events;
}

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

function setup(
  consumer: Record<string, unknown> | null = {},
  producer: Record<string, unknown> = {},
): TopologyInput {
  return {
    brokers: [{ kind: 'broker', id: 'b1' }],
    topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
    producers: [
      {
        kind: 'producer',
        id: 'p1',
        ratePerSec: 5,
        keyStrategy: { type: 'random', keys: ['A', 'B', 'C', 'D'] },
        ...producer,
      },
    ],
    consumers:
      consumer === null
        ? []
        : [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest', ...consumer }],
    connections: [
      { from: 'p1', to: 'orders' },
      ...(consumer === null ? [] : [{ from: 'orders', to: 'c1' }]),
    ],
  };
}

const addConsumerTo = (engine: Engine, extra: Record<string, unknown> = {}) => {
  engine.dispatch({ type: 'AddComponent', component: { kind: 'consumer', id: 'c1', ...extra } });
  engine.dispatch({ type: 'Connect', from: 'orders', to: 'c1' });
};

const addConsumer = (engine: Engine, extra: Record<string, unknown> = {}) => {
  engine.dispatch({ type: 'AddComponent', component: { kind: 'consumer', id: 'c1', ...extra } });
  engine.dispatch({ type: 'Connect', from: 'orders', to: 'c1' });
};

/** Offsets consumed per partition, in consumption order. */
function consumedOffsets(events: EngineEvent[]): Map<number, number[]> {
  const out = new Map<number, number[]>();
  for (const e of ofType(events, 'RecordConsumed'))
    out.set(e.partition, [...(out.get(e.partition) ?? []), e.offset]);
  return out;
}

describe('consumer poll loop', () => {
  it('reads every record exactly once, in offset order, per partition', () => {
    const engine = createEngine({ seed: 4, topology: setup() });
    const events = record(engine);
    engine.runUntil(10_000);
    engine.dispatch({ type: 'Disconnect', from: 'p1', to: 'orders' });
    engine.runUntil(11_000);
    const snap = engine.getSnapshot();
    const consumed = consumedOffsets(events);
    for (const p of snap.topics[0]?.partitions ?? []) {
      expect(consumed.get(p.index) ?? []).toEqual(
        Array.from({ length: p.logEndOffset }, (_, i) => i),
      );
    }
    expect(ofType(events, 'RecordConsumed')).toHaveLength(50);
  });

  it('never returns a record before the leader has appended it', () => {
    const engine = createEngine({ seed: 4, topology: setup() });
    const events = record(engine);
    engine.runUntil(5000);
    const appendedAt = new Map<string, number>();
    for (const e of events) {
      const key =
        'offset' in e && 'topic' in e
          ? `${e.topic}-${String(e.partition)}@${String(e.offset)}`
          : '';
      if (e.type === 'RecordAppended') appendedAt.set(key, e.seq);
      if (e.type === 'RecordConsumed') expect(appendedAt.get(key)).toBeLessThan(e.seq);
    }
  });

  it('max.poll.records caps one poll over all partitions together', () => {
    const engine = createEngine({ seed: 2, topology: setup(null, { ratePerSec: 10 }) });
    engine.runUntil(3000); // 30 records spread over 3 partitions
    const events = record(engine);
    addConsumer(engine, { autoOffsetReset: 'earliest', maxPollRecords: 10, processingTimeMs: 100 });
    engine.runUntil(3500);
    const consumed = ofType(events, 'RecordConsumed');
    // poll() at 3000 sends the fetch; the response arrives at 3002 and poll() returns 10 of them.
    expect(consumed.filter((e) => e.t === 3002)).toHaveLength(10);
    // The rest stay in the fetch buffer: each later poll() (every 10 × 100 ms) returns 10 more.
    expect(consumed.filter((e) => e.t === 4002)).toHaveLength(0);
    engine.runUntil(5100);
    expect(new Set(ofType(events, 'RecordConsumed').map((e) => e.t))).toEqual(
      new Set([3002, 4002, 5002]),
    );
  });

  it('serves every partition: buffered leftovers are returned before anything is fetched again', () => {
    const engine = createEngine({ seed: 2, topology: setup(null, { ratePerSec: 30 }) });
    engine.runUntil(3000);
    const events = record(engine);
    addConsumer(engine, { autoOffsetReset: 'earliest', maxPollRecords: 5, processingTimeMs: 10 });
    engine.runUntil(5000);
    const withData = (engine.getSnapshot().topics[0]?.partitions ?? [])
      .filter((p) => p.logEndOffset > 0)
      .map((p) => p.index);
    expect(withData.length).toBeGreaterThan(1);
    const partitionsSeen = [...new Set(ofType(events, 'RecordConsumed').map((e) => e.partition))];
    expect(partitionsSeen.sort()).toEqual(withData);
    // At most max.poll.records per poll() call.
    const perPoll = new Map<number, number>();
    for (const e of ofType(events, 'RecordConsumed')) perPoll.set(e.t, (perPoll.get(e.t) ?? 0) + 1);
    for (const n of perPoll.values()) expect(n).toBeLessThanOrEqual(5);
  });
});

describe('long-poll fetch (fetch.min.bytes, fetch.max.wait.ms)', () => {
  /** One partition, a producer that only sends manual records, and a consumer from offset 0. */
  function quiet(consumer: Record<string, unknown> | null = {}) {
    const engine = createEngine({
      seed: 1,
      topology: {
        brokers: [{ kind: 'broker', id: 'b1' }],
        topics: [{ kind: 'topic', id: 'orders', partitions: 1 }],
        // 1 record per 1000 s: in these tests only ProduceManual records exist.
        producers: [{ kind: 'producer', id: 'p1', ratePerSec: 0.001 }],
        consumers:
          consumer === null
            ? []
            : [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest', ...consumer }],
        connections: [
          { from: 'p1', to: 'orders' },
          ...(consumer === null ? [] : [{ from: 'orders', to: 'c1' }]),
        ],
      },
    });
    return { engine, events: record(engine) };
  }
  const produce = (engine: Engine, value = 'v') =>
    engine.dispatch({ type: 'ProduceManual', producerId: 'p1', key: null, value });

  it('answers at once when data is there: poll → +1 ms at the broker → +1 ms back', () => {
    const { engine, events } = quiet(null);
    engine.runUntil(100);
    produce(engine); // appended at 101
    engine.runUntil(500);
    addConsumerTo(engine, { autoOffsetReset: 'earliest' }); // first poll() at 500
    engine.runUntil(600);
    expect(ofType(events, 'RecordConsumed').map((e) => [e.t, e.offset])).toEqual([[502, 0]]);
  });

  it('holds the fetch until data arrives: consumed at append time + one network delay', () => {
    const { engine, events } = quiet();
    engine.runUntil(250);
    produce(engine); // sent at 250, appended at 251
    engine.runUntil(400);
    expect(ofType(events, 'RecordAppended').map((e) => e.t)).toEqual([251]);
    expect(ofType(events, 'RecordConsumed').map((e) => e.t)).toEqual([252]);
  });

  it('times out after fetch.max.wait.ms with an empty response, then fetches again', () => {
    const { engine, events } = quiet({ fetchMaxWaitMs: 300 });
    // The first fetch is parked at t=1 and expires at 301 with an empty answer, just before the
    // record sent at 300 is appended (also at 301). The answer reaches the consumer at 302; its
    // next fetch reaches the broker at 303, finds the record and is answered at once (304).
    engine.runUntil(300);
    produce(engine);
    engine.runUntil(1000);
    expect(ofType(events, 'RecordAppended').map((e) => e.t)).toEqual([301]);
    expect(ofType(events, 'RecordConsumed').map((e) => e.t)).toEqual([304]);
  });

  it('with fetch.max.wait.ms elapsed and too few bytes, the response carries what is there', () => {
    // fetch.min.bytes 2500 with 1024-byte records: one record is not enough to answer early.
    const { engine, events } = quiet({ fetchMinBytes: 2500, fetchMaxWaitMs: 500 });
    engine.runUntil(100);
    produce(engine); // appended at 101, but the fetch parked at t=1 waits until 501
    engine.runUntil(1000);
    expect(ofType(events, 'RecordConsumed').map((e) => e.t)).toEqual([502]);
  });

  it('accumulates until fetch.min.bytes are available, then answers immediately', () => {
    const { engine, events } = quiet({ fetchMinBytes: 2500, fetchMaxWaitMs: 500 });
    engine.runUntil(10);
    produce(engine, 'a'); // 1024 bytes at 11
    engine.runUntil(20);
    produce(engine, 'b'); // 2048 bytes at 21
    engine.runUntil(30);
    produce(engine, 'c'); // 3072 ≥ 2500 at 31: answered now, at the consumer at 32
    engine.runUntil(400);
    expect(ofType(events, 'RecordConsumed').map((e) => [e.t, e.value])).toEqual([
      [32, 'a'],
      [32, 'b'],
      [32, 'c'],
    ]);
  });

  it('a record from a fetch is consumed no earlier than one network delay after its append', () => {
    const engine = createEngine({ seed: 9, topology: setup({ processingTimeMs: 3 }) });
    const events = record(engine);
    engine.runUntil(10_000);
    const appendedAt = new Map(ofType(events, 'RecordAppended').map((e) => [e.traceId, e.t]));
    for (const e of ofType(events, 'RecordConsumed')) {
      expect(e.t).toBeGreaterThanOrEqual((appendedAt.get(e.traceId) ?? Infinity) + 1);
    }
  });

  it('prefetches while the application processes, so the next poll() returns at once', () => {
    // 5 records/s; each record takes 300 ms to process. The fetch sent when poll() returned the
    // first record completes during processing, so the next poll() returns from the buffer.
    const engine = createEngine({
      seed: 1,
      topology: setup(
        { processingTimeMs: 300, maxPollRecords: 1 },
        { ratePerSec: 5, keyStrategy: { type: 'fixed', key: 'A' } },
      ),
    });
    const events = record(engine);
    engine.runUntil(1000);
    expect(ofType(events, 'RecordConsumed').map((e) => [e.t, e.offset])).toEqual([
      [202, 0], // appended at 201, answered, at the consumer at 202
      [502, 1], // poll() 300 ms later: offset 1 (appended 401) was prefetched
      [802, 2], // offset 2 (appended 601) likewise
    ]);
  });

  it('a killed consumer ignores fetch responses that were in flight', () => {
    const { engine, events } = quiet();
    engine.runUntil(100);
    produce(engine); // appended at 101 → response in flight, due at the consumer at 102
    engine.runUntil(101);
    engine.dispatch({ type: 'KillConsumer', id: 'c1' });
    engine.runUntil(2000);
    expect(ofType(events, 'RecordConsumed')).toEqual([]);
    engine.dispatch({ type: 'RemoveComponent', id: 'c1' });
    engine.runUntil(3000);
    expect(ofType(events, 'RecordConsumed')).toEqual([]);
  });

  it('a removed and re-added consumer does not receive its predecessor’s parked fetch', () => {
    const { engine, events } = quiet({ enableAutoCommit: false });
    engine.runUntil(50); // c1's fetch is parked at the broker
    engine.dispatch({ type: 'RemoveComponent', id: 'c1' });
    addConsumerTo(engine, { autoOffsetReset: 'earliest', enableAutoCommit: false });
    engine.runUntil(100);
    produce(engine); // completes both the stale parked fetch and the new one
    engine.runUntil(1000);
    expect(ofType(events, 'RecordConsumed').map((e) => [e.t, e.offset])).toEqual([[102, 0]]);
  });
});

describe('auto.offset.reset (no committed offset yet)', () => {
  it('earliest starts at the log start offset: existing records are read', () => {
    const engine = createEngine({ seed: 3, topology: setup(null) });
    engine.runUntil(2000);
    const events = record(engine);
    addConsumer(engine, { autoOffsetReset: 'earliest' });
    engine.runUntil(2002);
    // 10 records were produced by t=2000. The one sent at 2000 is appended at 2001, just before
    // the consumer's first fetch arrives (also 2001), so the response at 2002 carries all 10.
    expect(ofType(events, 'RecordConsumed').map((e) => e.t)).toEqual(Array(10).fill(2002));
  });

  it('latest starts at the end (high watermark): records written before it joined are skipped', () => {
    const engine = createEngine({ seed: 3, topology: setup(null) });
    engine.runUntil(2000);
    const events = record(engine);
    addConsumer(engine, { autoOffsetReset: 'latest' });
    engine.runUntil(4000);
    const consumed = ofType(events, 'RecordConsumed');
    expect(consumed.length).toBeGreaterThan(0);
    expect(consumed.every((e) => e.t > 2000)).toBe(true);
    expect(consumed).toHaveLength(10); // only the 10 records produced after it joined
  });
});

describe('committing offsets', () => {
  it('auto-commits the positions from earlier polls once auto.commit.interval.ms has passed', () => {
    const engine = createEngine({ seed: 5, topology: setup({ autoCommitIntervalMs: 2000 }) });
    const events = record(engine);
    engine.runUntil(4500);
    const commits = ofType(events, 'OffsetCommitted');
    expect(commits.length).toBeGreaterThan(0);
    expect(new Set(commits.map((c) => c.t)).size).toBe(2); // one commit round at ≥2000, one at ≥4000
    for (const c of commits) {
      expect(c.t).toBeGreaterThanOrEqual(2000);
      // the committed offset is the next offset to read: one past a record already returned
      const returned = ofType(events, 'RecordConsumed').filter(
        (r) => r.partition === c.partition && r.seq < c.seq,
      );
      expect(c.offset).toBe((returned.at(-1)?.offset ?? -1) + 1);
    }
  });

  it('does not commit when enable.auto.commit is false', () => {
    const engine = createEngine({ seed: 5, topology: setup({ enableAutoCommit: false }) });
    const events = record(engine);
    engine.runUntil(20_000);
    expect(ofType(events, 'OffsetCommitted')).toEqual([]);
    expect(ofType(events, 'RecordConsumed').length).toBeGreaterThan(0);
  });
});

describe('committed offsets when a topic is deleted and re-created', () => {
  /** Every OffsetCommitted is at most the HW of the topic life it went to, at that moment. */
  function expectCommitsWithinHw(events: EngineEvent[]): void {
    const hw = new Map<string, number>();
    for (const e of events) {
      if (e.type === 'ComponentRemoved' && e.component.kind === 'topic') hw.clear();
      if (e.type === 'RecordAppended') hw.set(`${e.topic}-${String(e.partition)}`, e.offset + 1);
      if (e.type === 'OffsetCommitted') {
        expect(e.offset).toBeLessThanOrEqual(hw.get(`${e.topic}-${String(e.partition)}`) ?? 0);
      }
    }
  }

  // The counterexample fast-check found (invariants.prop.test.ts, seed -1172367150), shrunk.
  it('regression: a consumer dropping a deleted topic does not commit into its re-created namesake', () => {
    const engine = createEngine({
      seed: 0,
      topology: {
        brokers: [{ kind: 'broker', id: 'b1' }],
        topics: [{ kind: 'topic', id: 't0', partitions: 1 }],
        producers: [
          { kind: 'producer', id: 'p0', ratePerSec: 1, keyStrategy: { type: 'fixed', key: 'A' } },
        ],
        consumers: [
          {
            kind: 'consumer',
            id: 'c0',
            autoOffsetReset: 'earliest',
            autoCommitIntervalMs: 100,
            maxPollRecords: 1,
            fetchMinBytes: 0,
            fetchMaxWaitMs: 0,
            metadataMaxAgeMs: 0,
          },
        ],
        connections: [
          { from: 'p0', to: 't0' },
          { from: 't0', to: 'c0' },
        ],
      },
    });
    const events = record(engine);
    engine.runUntil(1002); // offset 0 is appended at 1001 and consumed at 1002: position 1
    engine.dispatch({ type: 'RemoveComponent', id: 't0' });
    engine.runUntil(1003);
    engine.dispatch({
      type: 'AddComponent',
      component: { kind: 'topic', id: 't0', partitions: 1 },
    });
    engine.runUntil(1004); // c0 polls, drops t0-0 and tries to commit position 1 on the way out

    expectCommitsWithinHw(events);
    const reAdded = events.findIndex((e) => e.type === 'ComponentAdded');
    expect(ofType(events.slice(reAdded), 'OffsetCommitted')).toEqual([]);
    expect(engine.getSnapshot().groups).toEqual([{ groupId: 'c0-group', committed: [] }]);
  });

  it('deleting a topic deletes the group’s committed offsets for it', () => {
    const engine = createEngine({ seed: 5, topology: setup({ autoCommitIntervalMs: 500 }) });
    engine.runUntil(5000);
    engine.dispatch({ type: 'RemoveComponent', id: 'c1' }); // close() commits
    const committed = () => engine.getSnapshot().groups[0]?.committed ?? [];
    expect(committed().length).toBeGreaterThan(0);

    engine.dispatch({ type: 'RemoveComponent', id: 'orders' });
    expect(committed()).toEqual([]);
  });

  it('a re-created topic is read from the start, not from the old topic’s committed offsets', () => {
    const engine = createEngine({ seed: 5, topology: setup({ autoCommitIntervalMs: 500 }) });
    const events = record(engine);
    engine.runUntil(5000);
    engine.dispatch({ type: 'RemoveComponent', id: 'c1' });
    engine.dispatch({ type: 'RemoveComponent', id: 'orders' });
    engine.dispatch({
      type: 'AddComponent',
      component: { kind: 'topic', id: 'orders', partitions: 3 },
    });
    engine.dispatch({ type: 'Connect', from: 'p1', to: 'orders' });
    engine.runUntil(15_000); // the new topic grows past the old committed offsets
    const start = events.length;
    addConsumer(engine, { autoOffsetReset: 'earliest' });
    engine.runUntil(20_000);

    for (const offsets of consumedOffsets(events.slice(start)).values()) {
      expect(offsets[0]).toBe(0);
    }
    expectCommitsWithinHw(events);
  });

  it('a consumer still subscribed across the re-creation resets and then commits again', () => {
    const engine = createEngine({ seed: 5, topology: setup({ autoCommitIntervalMs: 200 }) });
    const events = record(engine);
    engine.runUntil(5000);
    engine.dispatch({ type: 'RemoveComponent', id: 'orders' });
    engine.dispatch({
      type: 'AddComponent',
      component: { kind: 'topic', id: 'orders', partitions: 3 },
    });
    engine.dispatch({ type: 'Connect', from: 'p1', to: 'orders' });
    engine.dispatch({ type: 'Connect', from: 'orders', to: 'c1' });
    const start = events.length;
    engine.runUntil(10_000);

    expectCommitsWithinHw(events);
    const after = events.slice(start);
    for (const offsets of consumedOffsets(after).values()) expect(offsets[0]).toBe(0);
    expect(ofType(after, 'OffsetCommitted').length).toBeGreaterThan(0);
  });
});

describe('stopping and restarting a consumer', () => {
  it('a crash loses nothing that was produced while it was down, but replays records after the last commit', () => {
    const engine = createEngine({ seed: 6, topology: setup({ autoCommitIntervalMs: 3000 }) });
    const events = record(engine);
    engine.runUntil(4000); // committed at the first poll at/after 3000
    expect(engine.dispatch({ type: 'KillConsumer', id: 'c1' })).toEqual({ ok: true });
    engine.runUntil(8000); // records keep arriving; nobody reads them
    const beforeRestart = ofType(events, 'RecordConsumed').length;
    expect(engine.dispatch({ type: 'RestartConsumer', id: 'c1' })).toEqual({ ok: true });
    engine.runUntil(8002); // poll at 8000, fetch at the broker at 8001, response at 8002

    const consumed = ofType(events, 'RecordConsumed');
    const seen = new Map<string, number>();
    for (const e of consumed) {
      const k = `${String(e.partition)}@${String(e.offset)}`;
      seen.set(k, (seen.get(k) ?? 0) + 1);
    }
    const duplicates = [...seen.values()].filter((n) => n > 1).length;
    expect(duplicates).toBeGreaterThan(0); // at-least-once: records after the last commit come again
    const snap = engine.getSnapshot();
    const total = (snap.topics[0]?.partitions ?? []).reduce((sum, p) => sum + p.logEndOffset, 0);
    expect(seen.size).toBe(total); // nothing produced while it was down was lost
    expect(consumed.length).toBeGreaterThan(beforeRestart);
    expect(ofType(events, 'ConsumerStopped')).toHaveLength(1);
    expect(ofType(events, 'ConsumerStarted')).toHaveLength(1);
  });

  it('removing a consumer closes it gracefully: it commits first, so a re-added one has no duplicates', () => {
    const engine = createEngine({ seed: 6, topology: setup() });
    const events = record(engine);
    engine.runUntil(4000);
    engine.dispatch({ type: 'RemoveComponent', id: 'c1' });
    const lastBeforeRemove = ofType(events, 'OffsetCommitted').at(-1);
    expect(lastBeforeRemove?.t).toBe(4000); // close() committed right away (interval 5000 not reached)
    engine.runUntil(6000);
    addConsumer(engine, { autoOffsetReset: 'earliest' }); // same id, same default group "c1-group"
    engine.runUntil(9000);
    const seen = new Set<string>();
    for (const e of ofType(events, 'RecordConsumed')) {
      const k = `${String(e.partition)}@${String(e.offset)}`;
      expect(seen.has(k)).toBe(false);
      seen.add(k);
    }
  });

  it('rejects killing a stopped consumer and restarting a running one', () => {
    const engine = createEngine({ seed: 1, topology: setup() });
    expect(engine.dispatch({ type: 'RestartConsumer', id: 'c1' })).toMatchObject({
      ok: false,
      reason: /already running/,
    });
    engine.dispatch({ type: 'KillConsumer', id: 'c1' });
    expect(engine.dispatch({ type: 'KillConsumer', id: 'c1' })).toMatchObject({
      ok: false,
      reason: /already stopped/,
    });
    expect(engine.dispatch({ type: 'KillConsumer', id: 'p1' })).toMatchObject({
      ok: false,
      reason: /no consumer/,
    });
  });

  it('a slow consumer falls further and further behind', () => {
    // One record per poll, 500 ms each: 2 records/s against 5 records/s produced.
    const engine = createEngine({
      seed: 7,
      topology: setup({ processingTimeMs: 500, maxPollRecords: 1 }),
    });
    const events = record(engine);
    const behind: number[] = [];
    for (const t of [5000, 10_000, 15_000, 20_000]) {
      engine.runUntil(t);
      const produced = ofType(events, 'RecordAppended').length;
      behind.push(produced - ofType(events, 'RecordConsumed').length);
    }
    for (let i = 1; i < behind.length; i++) expect(behind[i]).toBeGreaterThan(behind[i - 1] ?? 0);
  });
});
