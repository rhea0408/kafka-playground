/**
 * Commits, crashes and delivery semantics (M3-T5, FR-2.8): what a crash does to a group under
 * each commit mode, shown by the DuplicateProcessed / RecordsSkipped teaching aid (M3 Q9).
 */

import type { TopologyInput } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import { createEngineInternal } from '../engine.ts';
import type { EngineEvent } from '../events/index.ts';
import type { ConsumerState } from '../model/state.ts';

const ofType = <T extends EngineEvent['type']>(events: EngineEvent[], type: T) =>
  events.filter((e): e is Extract<EngineEvent, { type: T }> => e.type === type);

/** c1 and c2 in group "g", one partition each of "orders" (2 partitions; keys A → 0, D → 1). */
function twoMembers(extra: Record<string, unknown>, c2Extra: Record<string, unknown> = {}) {
  const topology: TopologyInput = {
    brokers: [{ kind: 'broker', id: 'b1', groupInitialRebalanceDelayMs: 0 }],
    topics: [{ kind: 'topic', id: 'orders', partitions: 2 }],
    producers: [
      {
        kind: 'producer',
        id: 'p1',
        ratePerSec: 20,
        keyStrategy: { type: 'random', keys: ['A', 'D'] },
      },
    ],
    consumers: [
      { kind: 'consumer', id: 'c1', groupId: 'g', autoOffsetReset: 'earliest', ...extra },
      {
        kind: 'consumer',
        id: 'c2',
        groupId: 'g',
        autoOffsetReset: 'earliest',
        ...extra,
        ...c2Extra,
      },
    ],
    connections: [
      { from: 'p1', to: 'orders' },
      { from: 'orders', to: 'c1' },
      { from: 'orders', to: 'c2' },
    ],
  };
  const internal = createEngineInternal({ seed: 3, topology });
  const events: EngineEvent[] = [];
  internal.engine.on((e) => events.push(e));
  const consumer = (id: string): ConsumerState => {
    const c = internal.context.state.consumers.get(id);
    if (c === undefined) throw new Error(id);
    return c;
  };
  const committed = (key: string) => internal.context.state.groups.get('g')?.committed.get(key);
  return { ...internal, events, consumer, committed };
}

/** Steps the engine until c1 is part-way through a batch (some records processed, some not). */
function untilMidBatch(h: ReturnType<typeof twoMembers>, perRecordMs: number): void {
  const c1 = h.consumer('c1');
  for (let t = h.engine.now(); t < 60_000; t += 10) {
    h.engine.runUntil(t);
    const { batch } = c1;
    if (batch?.startedAt === undefined || batch.startedAt === null) continue;
    const done = Math.floor((t - batch.startedAt) / perRecordMs);
    if (batch.records.length >= 3 && done >= 1 && done < batch.records.length) return;
  }
  throw new Error('c1 never got a multi-record batch');
}

/** The partition (key "orders-n") c1 owns. */
const c1Partition = (h: ReturnType<typeof twoMembers>): string => {
  const tp = h.consumer('c1').group.owned[0];
  if (tp === undefined) throw new Error('c1 owns nothing');
  return `${tp.topic}-${String(tp.partition)}`;
};

describe('delivery semantics after a crash', () => {
  it('auto-commit: the new owner re-reads what c1 processed after its last commit (duplicates)', () => {
    const h = twoMembers({ processingTimeMs: 100, maxPollRecords: 5 });
    h.engine.runUntil(7000); // c1 has auto-committed at least once
    untilMidBatch(h, 100);
    const key = c1Partition(h);
    const committedAtCrash = h.committed(key) ?? 0;
    h.engine.dispatch({ type: 'KillConsumer', id: 'c1' });
    const processedAtCrash =
      h.context.state.groups.get('g')?.processed.get(key) ?? committedAtCrash - 1;
    h.engine.runUntil(25_000); // c2 takes over after c1's session timeout

    const partition = Number(key.slice(key.lastIndexOf('-') + 1));
    const dups = ofType(h.events, 'DuplicateProcessed').filter((e) => e.partition === partition);
    expect(processedAtCrash).toBeGreaterThanOrEqual(committedAtCrash);
    expect(dups.map((e) => e.offset)).toEqual(
      Array.from(
        { length: processedAtCrash - committedAtCrash + 1 },
        (_, i) => committedAtCrash + i,
      ),
    );
    expect(dups.every((e) => e.consumerId === 'c2')).toBe(true);
    expect(ofType(h.events, 'RecordsSkipped')).toEqual([]);
  });

  it('sync-before-processing: records committed but not processed are skipped (at-most-once)', () => {
    const h = twoMembers({
      processingTimeMs: 100,
      maxPollRecords: 5,
      commitMode: 'sync-before-processing',
    });
    h.engine.runUntil(3000);
    untilMidBatch(h, 100);
    const key = c1Partition(h);
    h.engine.dispatch({ type: 'KillConsumer', id: 'c1' });
    const processedAtCrash = h.context.state.groups.get('g')?.processed.get(key) ?? -1;
    const committedAtCrash = h.committed(key) ?? 0;
    h.engine.runUntil(25_000);

    const partition = Number(key.slice(key.lastIndexOf('-') + 1));
    const skipped = ofType(h.events, 'RecordsSkipped').filter((e) => e.partition === partition);
    expect(skipped).toMatchObject([
      { consumerId: 'c2', from: processedAtCrash + 1, to: committedAtCrash - 1 },
    ]);
    expect(ofType(h.events, 'DuplicateProcessed').filter((e) => e.partition === partition)).toEqual(
      [],
    );
  });

  it('a graceful stop finishes the batch and commits: no duplicates and no skips', () => {
    const h = twoMembers({ processingTimeMs: 100, maxPollRecords: 5 });
    h.engine.runUntil(7000);
    untilMidBatch(h, 100);
    expect(h.engine.dispatch({ type: 'StopConsumer', id: 'c1' })).toEqual({ ok: true });
    expect(h.engine.dispatch({ type: 'StopConsumer', id: 'c1' })).toMatchObject({
      ok: false,
      reason: /already stopping/,
    });
    expect(h.consumer('c1').status).toBe('running'); // still finishing its batch
    h.engine.runUntil(12_000);
    expect(h.consumer('c1').status).toBe('stopped');
    expect(ofType(h.events, 'ConsumerStopped')).toMatchObject([
      { consumerId: 'c1', reason: 'close' },
    ]);
    // LeaveGroup: the group rebalances at once instead of waiting for a session timeout.
    expect(ofType(h.events, 'MemberRemoved')).toMatchObject([
      { consumerId: 'c1', reason: 'leave' },
    ]);
    expect(ofType(h.events, 'DuplicateProcessed')).toEqual([]);
    expect(ofType(h.events, 'RecordsSkipped')).toEqual([]);
  });

  it('a stopped or crashed consumer can be restarted; commands check the status', () => {
    const h = twoMembers({});
    h.engine.runUntil(2000);
    expect(h.engine.dispatch({ type: 'KillConsumer', id: 'c1' })).toEqual({ ok: true });
    expect(h.consumer('c1').status).toBe('crashed');
    expect(h.engine.dispatch({ type: 'StopConsumer', id: 'c1' })).toMatchObject({ ok: false });
    expect(h.engine.dispatch({ type: 'KillConsumer', id: 'c1' })).toMatchObject({ ok: false });
    expect(h.engine.dispatch({ type: 'RestartConsumer', id: 'c1' })).toEqual({ ok: true });
    h.engine.runUntil(2001);
    // Blocked in poll() (or between polls with nothing to do): StopConsumer closes at once.
    h.engine.dispatch({ type: 'StopConsumer', id: 'c2' });
    expect(h.consumer('c2').status === 'stopped' || h.consumer('c2').stopRequested).toBe(true);
  });
});

describe('commit modes', () => {
  it('sync-after-processing commits after each batch and polls only after the answer', () => {
    const h = twoMembers({
      processingTimeMs: 50,
      maxPollRecords: 2,
      commitMode: 'sync-after-processing',
      enableAutoCommit: false,
    });
    h.engine.runUntil(5000);
    const commits = ofType(h.events, 'OffsetCommitted').filter((e) => e.consumerId === 'c1');
    expect(commits.length).toBeGreaterThan(5);
    // Each batch's commit lands 1 ms after processing ends; the next records come after that.
    const consumed = ofType(h.events, 'RecordConsumed').filter((e) => e.consumerId === 'c1');
    for (const c of commits) {
      expect(consumed.some((r) => r.t === c.t)).toBe(false); // nothing returned while blocked
    }
  });

  it('async-after-processing commits without blocking the next poll()', () => {
    const h = twoMembers({
      processingTimeMs: 50,
      maxPollRecords: 2,
      commitMode: 'async-after-processing',
      enableAutoCommit: false,
    });
    h.engine.runUntil(5000);
    expect(
      ofType(h.events, 'OffsetCommitted').filter((e) => e.consumerId === 'c1').length,
    ).toBeGreaterThan(5);
  });

  it('auto with enable.auto.commit=false never commits, so a rebalance replays from the start', () => {
    const h = twoMembers({ enableAutoCommit: false });
    h.engine.runUntil(5000);
    expect(ofType(h.events, 'OffsetCommitted')).toEqual([]);
    h.engine.dispatch({ type: 'StopConsumer', id: 'c2' });
    h.engine.runUntil(8000);
    // c1 takes c2's partition over from offset 0 (earliest): everything there is a duplicate.
    expect(ofType(h.events, 'DuplicateProcessed').length).toBeGreaterThan(0);
  });
});

describe('commits and generations', () => {
  it('a commit from an old generation is refused with ILLEGAL_GENERATION', () => {
    const h = twoMembers({});
    h.engine.runUntil(2000);
    const c1 = h.consumer('c1');
    h.kernel.scheduleIn(0, {
      type: 'CoordinatorRequest',
      groupId: 'g',
      consumerId: 'c1',
      incarnation: c1.incarnation,
      request: {
        api: 'OffsetCommit',
        commitId: 999,
        memberId: c1.group.memberId,
        generation: c1.group.generation - 1,
        offsets: [{ topic: 'orders', partition: 0, offset: 1, topicId: 1 }],
      },
    });
    h.engine.runUntil(2010);
    expect(ofType(h.events, 'CommitFailed')).toMatchObject([
      { consumerId: 'c1', error: 'ILLEGAL_GENERATION' },
    ]);
  });

  it('a zombie’s commit is refused while the new owner’s commits are accepted', () => {
    // c2 takes 20 s per record and leaves after 15 s without poll(); its commitSync() after the
    // batch fails because it is no longer a member (CommitFailedException), while c1, now owning
    // c2's partition, commits it normally.
    const h = twoMembers(
      { maxPollIntervalMs: 15_000, commitMode: 'sync-after-processing', enableAutoCommit: false },
      { processingTimeMs: 20_000, maxPollRecords: 1 },
    );
    h.engine.runUntil(40_000);
    const removed = ofType(h.events, 'MemberRemoved').find((e) => e.consumerId === 'c2');
    expect(removed?.reason).toBe('poll-timeout');
    const zombieFail = ofType(h.events, 'CommitFailed').find((e) => e.consumerId === 'c2');
    expect(zombieFail).toMatchObject({ error: 'COMMIT_FAILED' });
    const c2Partition = ofType(h.events, 'RecordConsumed').find(
      (e) => e.consumerId === 'c2',
    )?.partition;
    const newOwnerCommits = ofType(h.events, 'OffsetCommitted').filter(
      (e) => e.consumerId === 'c1' && e.partition === c2Partition && e.t > (removed?.t ?? 0),
    );
    expect(newOwnerCommits.length).toBeGreaterThan(0);
    // The zombie processed its record after c1 had taken over: a duplicate.
    expect(ofType(h.events, 'DuplicateProcessed').some((e) => e.consumerId === 'c1')).toBe(true);
  });
});
