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

  it('an empty poll waits pollIntervalMs; a non-empty poll waits records × processingTimeMs', () => {
    const engine = createEngine({
      seed: 1,
      topology: setup(
        { pollIntervalMs: 250, processingTimeMs: 40 },
        { ratePerSec: 1, keyStrategy: { type: 'fixed', key: 'A' } },
      ),
    });
    const events = record(engine);
    engine.runUntil(2400);
    // Records are appended at 1001 and 2001. Empty polls run every 250 ms: 0, 250, …, 1000, 1250.
    // The poll at 1250 returns offset 0; processing it takes 40 ms, so the next poll is at 1290,
    // then empty polls every 250 ms again: 1540, 1790, 2040 (finds offset 1).
    expect(ofType(events, 'RecordConsumed').map((e) => [e.t, e.offset])).toEqual([
      [1250, 0],
      [2040, 1],
    ]);
  });

  it('max.poll.records caps one poll over all partitions together', () => {
    const engine = createEngine({ seed: 2, topology: setup(null, { ratePerSec: 10 }) });
    engine.runUntil(3000); // 30 records spread over 3 partitions
    const events = record(engine);
    addConsumer(engine, { autoOffsetReset: 'earliest', maxPollRecords: 10, processingTimeMs: 100 });
    engine.runUntil(3000);
    const firstPoll = ofType(events, 'RecordConsumed').filter((e) => e.t === 3000);
    expect(firstPoll).toHaveLength(10);
  });

  it('rotates the partition it reads first, so one busy partition cannot starve the others', () => {
    const engine = createEngine({ seed: 2, topology: setup(null, { ratePerSec: 30 }) });
    engine.runUntil(3000);
    const events = record(engine);
    addConsumer(engine, { autoOffsetReset: 'earliest', maxPollRecords: 5, processingTimeMs: 10 });
    engine.runUntil(3100);
    const withData = (engine.getSnapshot().topics[0]?.partitions ?? [])
      .filter((p) => p.logEndOffset > 0)
      .map((p) => p.index);
    expect(withData.length).toBeGreaterThan(1);
    const partitionsSeen = [...new Set(ofType(events, 'RecordConsumed').map((e) => e.partition))];
    expect(partitionsSeen.sort()).toEqual(withData);
  });
});

describe('auto.offset.reset (no committed offset yet)', () => {
  it('earliest starts at the log start offset: existing records are read', () => {
    const engine = createEngine({ seed: 3, topology: setup(null) });
    engine.runUntil(2000);
    const events = record(engine);
    addConsumer(engine, { autoOffsetReset: 'earliest' });
    engine.runUntil(2000);
    // 10 records were produced by t=2000, but the one sent at t=2000 reaches the broker at 2001.
    expect(ofType(events, 'RecordConsumed')).toHaveLength(9);
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

describe('stopping and restarting a consumer', () => {
  it('a crash loses nothing that was produced while it was down, but replays records after the last commit', () => {
    const engine = createEngine({ seed: 6, topology: setup({ autoCommitIntervalMs: 3000 }) });
    const events = record(engine);
    engine.runUntil(4000); // committed at the first poll at/after 3000
    expect(engine.dispatch({ type: 'KillConsumer', id: 'c1' })).toEqual({ ok: true });
    engine.runUntil(8000); // records keep arriving; nobody reads them
    const beforeRestart = ofType(events, 'RecordConsumed').length;
    expect(engine.dispatch({ type: 'RestartConsumer', id: 'c1' })).toEqual({ ok: true });
    engine.runUntil(8000);

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
