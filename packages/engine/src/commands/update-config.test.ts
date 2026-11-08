import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngine, replayCommandLog, type Engine } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';
import { partitionForKey } from '../protocols/partitioner.ts';

function record(engine: Engine): EngineEvent[] {
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  return events;
}

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

function topology(
  producer: Record<string, unknown> = {},
  consumer: Record<string, unknown> = {},
): TopologyInput {
  return {
    // No initial rebalance delay, so consumers own their partitions 4 ms after starting.
    brokers: [{ kind: 'broker', id: 'b1', groupInitialRebalanceDelayMs: 0 }],
    topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
    producers: [{ kind: 'producer', id: 'p1', ratePerSec: 2, ...producer }],
    consumers: [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest', ...consumer }],
    connections: [
      { from: 'p1', to: 'orders' },
      { from: 'orders', to: 'c1' },
    ],
  };
}

describe('UpdateConfig: general rules', () => {
  it('emits ConfigUpdated with the full config before and after, and logs the command', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    const events = record(engine);
    engine.runUntil(100);
    const command = { type: 'UpdateConfig', id: 'p1', patch: { acks: '1' } } as const;
    expect(engine.dispatch(command)).toEqual({ ok: true });
    const [updated] = ofType(events, 'ConfigUpdated');
    expect(updated).toMatchObject({
      t: 100,
      id: 'p1',
      before: { acks: 'all' },
      after: { acks: '1' },
    });
    expect(updated?.after).toEqual({ ...updated?.before, acks: '1' });
    expect(engine.commandLog.at(-1)).toMatchObject({ t: 100, command });
    expect(engine.getSnapshot().producers[0]?.config.acks).toBe('1');
  });

  it('accepts a patch that changes nothing, without an event or a log entry', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    const events = record(engine);
    expect(
      engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 3 } }),
    ).toEqual({ ok: true });
    expect(engine.dispatch({ type: 'UpdateConfig', id: 'p1', patch: {} })).toEqual({ ok: true });
    expect(events).toEqual([]);
    expect(engine.commandLog).toEqual([]);
  });

  it('rejects unknown ids, id or kind changes, unknown fields and invalid values', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    const events = record(engine);
    const reasons = [
      { type: 'UpdateConfig', id: 'ghost', patch: {} },
      { type: 'UpdateConfig', id: 'p1', patch: { id: 'p2' } },
      { type: 'UpdateConfig', id: 'p1', patch: { kind: 'consumer' } },
      { type: 'UpdateConfig', id: 'p1', patch: { lingerMs: 5 } },
      { type: 'UpdateConfig', id: 'p1', patch: { ratePerSec: 0 } },
      { type: 'UpdateConfig', id: 'orders', patch: { partitions: 1.5 } },
    ].map((c) => {
      const result = engine.dispatch(c as Parameters<Engine['dispatch']>[0]);
      expect(result.ok).toBe(false);
      return result.ok ? '' : result.reason;
    });
    expect(reasons).toEqual([
      'There is no component with id "ghost".',
      expect.stringMatching(/Ids cannot be changed/),
      expect.stringMatching(/kind cannot be changed/),
      'A producer has no config field "lingerMs".',
      'ratePerSec: Rate must be greater than 0.',
      'partitions: Partitions must be a whole number.',
    ]);
    expect(events).toEqual([]);
    expect(engine.commandLog).toEqual([]);
  });

  it('rejects broker config changes: broker.rack is a static config', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    expect(
      engine.dispatch({ type: 'UpdateConfig', id: 'b1', patch: { rack: 'az1' } }),
    ).toMatchObject({ ok: false, reason: /^broker\.rack is a static broker config/ });
    expect(
      engine.dispatch({
        type: 'UpdateConfig',
        id: 'b1',
        patch: { groupInitialRebalanceDelayMs: 1000 },
      }),
    ).toMatchObject({
      ok: false,
      reason: /^group\.initial\.rebalance\.delay\.ms is a static broker config/,
    });
  });

  it('accepts the M3 consumer group fields and checks them with the schema', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    expect(
      engine.dispatch({
        type: 'UpdateConfig',
        id: 'c1',
        patch: {
          sessionTimeoutMs: 20_000,
          maxPollIntervalMs: 60_000,
          partitionAssignmentStrategy: ['cooperative-sticky'],
          commitMode: 'sync-after-processing',
        },
      }),
    ).toEqual({ ok: true });
    expect(
      engine.dispatch({ type: 'UpdateConfig', id: 'c1', patch: { heartbeatIntervalMs: 20_000 } }),
    ).toEqual({
      ok: false,
      reason: 'heartbeatIntervalMs: heartbeat.interval.ms must be lower than session.timeout.ms.',
    });
  });
});

describe('UpdateConfig: topic partitions', () => {
  it('rejects a decrease with Kafka’s InvalidPartitionsException wording', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    expect(
      engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 2 } }),
    ).toEqual({
      ok: false,
      reason:
        'The topic orders currently has 3 partition(s); 2 would not be an increase. Kafka can add partitions to a topic but never remove them.',
    });
    expect(engine.getSnapshot().topics[0]?.partitions).toHaveLength(3);
  });

  it('rejects a replication-factor change but accepts min.insync.replicas', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    expect(
      engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { replicationFactor: 2 } }),
    ).toMatchObject({ ok: false });
    expect(
      engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { minInsyncReplicas: 2 } }),
    ).toEqual({ ok: true });
  });

  it('adds empty partitions on the broker and leaves existing records in place', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    engine.runUntil(5000);
    const before = engine.getSnapshot().topics[0]?.partitions ?? [];
    expect(
      engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 5 } }),
    ).toEqual({ ok: true });
    const after = engine.getSnapshot().topics[0]?.partitions ?? [];
    expect(after.slice(0, 3)).toEqual(before);
    const empty = (index: number) => ({
      index,
      leader: 'b1',
      replicas: [{ broker: 'b1', logEndOffset: 0, highWatermark: 0 }],
      isr: ['b1'],
      elr: [],
      underReplicated: false,
      underMinIsr: false,
      leaderEpoch: 0,
      partitionEpoch: 0,
      logStartOffset: 0,
      logEndOffset: 0,
      highWatermark: 0,
      records: [],
    });
    expect(after.slice(3)).toEqual([empty(3), empty(4)]);
  });

  it('remaps a key only after the producer refreshes its metadata; old records stay put', () => {
    // Key "B": murmur2 puts it on partition 1 of 3 and on partition 0 of 4.
    expect([partitionForKey('B', 3), partitionForKey('B', 4)]).toEqual([1, 0]);
    const engine = createEngine({
      seed: 1,
      topology: topology({ keyStrategy: { type: 'fixed', key: 'B' }, metadataMaxAgeMs: 10_000 }),
    });
    const events = record(engine);
    engine.runUntil(3000); // B records at 500 … 3000 → orders-1
    engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 4 } });
    engine.runUntil(9999); // metadata still says 3 partitions: B keeps going to orders-1
    engine.runUntil(12_000); // refreshed at the first send at/after 10000: B → orders-0

    const produced = ofType(events, 'RecordProduced');
    expect(produced.filter((e) => e.t < 10_000).every((e) => e.partition === 1)).toBe(true);
    expect(produced.filter((e) => e.t >= 10_000).every((e) => e.partition === 0)).toBe(true);
    expect(produced.filter((e) => e.t >= 10_000).length).toBeGreaterThan(0);

    const partitions = engine.getSnapshot().topics[0]?.partitions ?? [];
    // 19 B records (t = 500 … 9500) are still in orders-1; nothing moved. The 4 newer ones
    // (10000 … 11500; the one sent at 12000 is appended at 12001) are in orders-0.
    expect(partitions.map((p) => p.logEndOffset)).toEqual([4, 19, 0, 0]);
    // A consumer reading both partitions sees B's old records and new ones in separate logs, so
    // per-key order across the change is no longer guaranteed.
    const consumed = ofType(events, 'RecordConsumed').filter((e) => e.key === 'B');
    expect(new Set(consumed.map((e) => e.partition))).toEqual(new Set([0, 1]));
  });

  it('a consumer picks up new partitions at its metadata refresh; with "latest" it skips what came before', () => {
    // Key "A" is on partition 1 of 3 and on the new partition 3 of 5. The producer refreshes at
    // 10 s, the consumer only at 20 s: records written to the new partition in between are
    // skipped by a consumer that starts it at the end (latest), a known Kafka pitfall.
    expect([partitionForKey('A', 3), partitionForKey('A', 5)]).toEqual([1, 3]);
    const engine = createEngine({
      seed: 1,
      topology: topology(
        { keyStrategy: { type: 'fixed', key: 'A' }, metadataMaxAgeMs: 10_000 },
        { autoOffsetReset: 'latest', metadataMaxAgeMs: 20_000 },
      ),
    });
    const events = record(engine);
    engine.runUntil(1000);
    engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 5 } });
    engine.runUntil(25_000);
    const consumedFromNew = ofType(events, 'RecordConsumed').filter((e) => e.partition === 3);
    const appendedToNew = ofType(events, 'RecordAppended').filter((e) => e.partition === 3);
    expect(appendedToNew[0]?.t).toBe(10_001);
    expect(consumedFromNew.length).toBeGreaterThan(0);
    expect(Math.min(...consumedFromNew.map((e) => e.offset))).toBeGreaterThan(0);
    expect(consumedFromNew.every((e) => e.t > 20_000)).toBe(true);
  });

  it('null-key records: the sticky partitioner starts afresh for the topic after the refresh', () => {
    const engine = createEngine({
      seed: 3,
      topology: topology({ ratePerSec: 20, batchSize: 1_000_000, metadataMaxAgeMs: 1000 }),
    });
    const events = record(engine);
    engine.runUntil(500);
    engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 40 } });
    engine.runUntil(3000);
    const produced = ofType(events, 'RecordProduced');
    // batch.size is huge, so without a reset the producer would never leave its first partition.
    const early = new Set(produced.filter((e) => e.t < 1000).map((e) => e.partition));
    const late = new Set(produced.filter((e) => e.t >= 1000).map((e) => e.partition));
    expect(early.size).toBe(1);
    expect(late.size).toBe(1);
    expect([...late][0]).not.toBe([...early][0]); // seed 3 draws a different partition of 40
  });
});

describe('UpdateConfig: producer', () => {
  it('a rate change restarts the schedule from now and keeps the stats', () => {
    const engine = createEngine({ seed: 1, topology: topology({ ratePerSec: 2 }) });
    const events = record(engine);
    engine.runUntil(1200); // records at 500, 1000
    engine.dispatch({ type: 'UpdateConfig', id: 'p1', patch: { ratePerSec: 4 } });
    engine.runUntil(2000);
    expect(ofType(events, 'RecordProduced').map((e) => e.t)).toEqual([500, 1000, 1450, 1700, 1950]);
    expect(engine.getSnapshot().producers[0]?.stats).toMatchObject({ sent: 5 });
    // Trace ids keep counting in the same producer life.
    expect(ofType(events, 'RecordProduced').map((e) => e.traceId)).toEqual(
      [1, 2, 3, 4, 5].map((n) => `p1@1#${String(n)}`),
    );
  });

  it('in-flight requests keep their acks; new records use the new acks', () => {
    const engine = createEngine({ seed: 1, topology: topology({ acks: 'all' }) });
    const events = record(engine);
    engine.runUntil(500); // record 1 sent at 500 with acks=all; append 501, ack 502
    engine.dispatch({ type: 'UpdateConfig', id: 'p1', patch: { acks: '0' } });
    engine.runUntil(1000);
    expect(ofType(events, 'ProduceAcked').map((e) => [e.t, e.traceId, e.offset])).toEqual([
      [502, 'p1@1#1', 0],
      [1000, 'p1@1#2', null], // acks=0: acked at send time
    ]);
  });

  it('a new message size applies to the next record', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    engine.runUntil(600);
    engine.dispatch({ type: 'UpdateConfig', id: 'p1', patch: { messageSizeBytes: 10 } });
    engine.runUntil(1100);
    const records = (engine.getSnapshot().topics[0]?.partitions ?? []).flatMap((p) => p.records);
    expect(records.map((r) => r.sizeBytes).sort()).toEqual([10, 1024]);
  });
});

describe('UpdateConfig: consumer', () => {
  it('a group.id change leaves the old group (with a commit) and joins the new one (M3 Q5)', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    const events = record(engine);
    engine.runUntil(3000);
    const committedBefore = ofType(events, 'OffsetCommitted').length;
    expect(
      engine.dispatch({ type: 'UpdateConfig', id: 'c1', patch: { groupId: 'other' } }),
    ).toEqual({ ok: true });
    engine.runUntil(4000);
    expect(ofType(events, 'OffsetCommitted').length).toBeGreaterThan(committedBefore);
    expect(ofType(events, 'MemberRemoved')).toMatchObject([
      { groupId: 'c1-group', consumerId: 'c1', reason: 'leave' },
    ]);
    expect(ofType(events, 'MemberJoined').map((e) => e.groupId)).toEqual(['c1-group', 'other']);
    // The new group has no committed offsets, so auto.offset.reset (earliest) decides.
    const fromNewGroup = ofType(events, 'RecordConsumed').filter((e) => e.groupId === 'other');
    expect(Math.min(...fromNewGroup.map((e) => e.offset))).toBe(0);
  });

  it('auto.offset.reset changes are accepted and used for partitions assigned later', () => {
    const engine = createEngine({ seed: 1, topology: topology() });
    expect(
      engine.dispatch({ type: 'UpdateConfig', id: 'c1', patch: { autoOffsetReset: 'latest' } }),
    ).toEqual({ ok: true });
  });

  it('max.poll.records and processing time apply from the next poll', () => {
    const engine = createEngine({ seed: 1, topology: topology({ ratePerSec: 20 }) });
    engine.runUntil(5000);
    engine.dispatch({ type: 'KillConsumer', id: 'c1' });
    engine.runUntil(10_000);
    const events = record(engine);
    engine.dispatch({
      type: 'UpdateConfig',
      id: 'c1',
      patch: { maxPollRecords: 3, processingTimeMs: 100 },
    });
    engine.dispatch({ type: 'RestartConsumer', id: 'c1' });
    // The restarted member gets partitions once the crashed one's session has timed out (~13 s).
    engine.runUntil(14_000);
    const perPoll = new Map<number, number>();
    for (const e of ofType(events, 'RecordConsumed')) perPoll.set(e.t, (perPoll.get(e.t) ?? 0) + 1);
    // Each poll() returns 3 records and takes 300 ms to process.
    const [first, second] = [...perPoll.entries()];
    expect([first?.[1], second?.[1]]).toEqual([3, 3]);
    expect((second?.[0] ?? 0) - (first?.[0] ?? 0)).toBe(300);
    for (const n of perPoll.values()) expect(n).toBeLessThanOrEqual(3);
  });

  it('a fetch already parked at the broker keeps the fetch.max.wait.ms it was sent with', () => {
    const engine = createEngine({
      seed: 1,
      topology: { ...topology(), producers: [], connections: [{ from: 'orders', to: 'c1' }] },
    });
    engine.runUntil(10); // the first fetch (sent once c1 owns the partition, t=4) is parked at 5
    engine.dispatch({ type: 'UpdateConfig', id: 'c1', patch: { fetchMaxWaitMs: 100 } });
    // The parked fetch still expires at 505 (answer at 506); later fetches wait 100 ms.
    const steps: number[] = [];
    while (engine.now() < 900 && engine.step()) steps.push(engine.now());
    expect(steps).toContain(505);
    expect(steps).toContain(607); // second fetch parked at 507, expires after 100 ms
  });

  it('replays exactly, including UpdateConfig', () => {
    const options = {
      seed: 4,
      topology: topology({ keyStrategy: { type: 'random', keys: ['A', 'B', 'D'] } }),
    };
    const engine = createEngine(options);
    const events = record(engine);
    engine.runUntil(2000);
    engine.dispatch({ type: 'UpdateConfig', id: 'orders', patch: { partitions: 6 } });
    engine.dispatch({
      type: 'UpdateConfig',
      id: 'p1',
      patch: { ratePerSec: 7, metadataMaxAgeMs: 0 },
    });
    engine.runUntil(4000);
    engine.dispatch({ type: 'UpdateConfig', id: 'c1', patch: { fetchMinBytes: 5000 } });
    engine.runUntil(8000);
    const replayed: EngineEvent[] = [];
    replayCommandLog(options, engine.commandLog, (e) => replayed.push(e)).runUntil(8000);
    expect(JSON.stringify(replayed)).toBe(JSON.stringify(events));
  });
});
