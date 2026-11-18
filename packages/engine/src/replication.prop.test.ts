/**
 * Replication invariants (M4-T3, architecture §4.6), over the general random topologies (1–3
 * brokers, RF up to 3, minISR 1–3) and command sequences, which include broker kills, controlled
 * stops, restarts and topics deleted and re-created. M4-T9 adds the election and truncation rules.
 */

import type { TopologyInput } from '@kp/schema';
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { createEngineInternal } from './engine.ts';
import type { EngineEvent } from './events/index.ts';
import type { EngineContext } from './model/context.ts';
import { canServe } from './protocols/controller.ts';
import type { Command } from './commands/index.ts';
import { stepsArb, topologyArb, type Step } from './testing/arbitraries.ts';
import { runsFromEnv } from './testing/runs.ts';

const seedArb = fc.integer({ min: 0, max: 0xffffffff });
/** KP_INVARIANT_RUNS, default 100; the nightly job runs 1000. */
const NUM_RUNS = runsFromEnv('KP_INVARIANT_RUNS', 100);

function run(seed: number, topology: TopologyInput, steps: readonly Step[]) {
  const { engine, context } = createEngineInternal({ seed, topology });
  const events: EngineEvent[] = [];
  engine.on((e) => events.push(e));
  for (const step of steps) {
    if (step.kind === 'advance') engine.runUntil(engine.now() + step.ms);
    else engine.dispatch(step.command);
  }
  return { engine, context, events };
}

/** Running, registered and unfenced in this life: a broker that serves and fetches. */
const alive = (ctx: EngineContext, id: string) => canServe(ctx, id);

describe(
  'replication invariants (fast-check)',
  { timeout: Math.max(30_000, NUM_RUNS * 400) },
  () => {
    it('every acknowledged offset is where a leader appended that record', () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { events } = run(seed, topology, steps);
          // Retries can append a record again (acks=1 has no idempotence; a deleted and re-created
          // topic is a new log), so an ack matches one of the record's appends (M4-T7).
          const appended = new Map<string, Set<number>>();
          for (const e of events) {
            if (e.type === 'RecordAppended') {
              const set = appended.get(e.traceId) ?? new Set<number>();
              set.add(e.offset);
              appended.set(e.traceId, set);
            }
            if (e.type === 'ProduceAcked' && e.offset !== null) {
              expect(appended.get(e.traceId)?.has(e.offset)).toBe(true);
            }
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });

    it("an idempotent producer's records keep their order in every log (sequences increase per epoch)", () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { context } = run(seed, topology, steps);
          for (const topic of context.state.topics.values()) {
            for (const p of topic.partitions) {
              for (const replica of p.replicaLogs.values()) {
                const last = new Map<string, number>();
                for (const r of replica.log) {
                  if (r.pid < 0) continue;
                  const k = `${String(r.pid)}/${String(r.producerEpoch)}`;
                  expect(r.sequence).toBeGreaterThan(last.get(k) ?? -1);
                  last.set(k, r.sequence);
                }
              }
            }
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });

    it('an idempotent (acks=all) producer never writes a record twice into one log', () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { context } = run(seed, topology, steps);
          for (const topic of context.state.topics.values()) {
            for (const p of topic.partitions) {
              for (const replica of p.replicaLogs.values()) {
                const ids = replica.log.filter((r) => r.pid >= 0).map((r) => r.traceId);
                expect(new Set(ids).size).toBe(ids.length);
              }
            }
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });

    it('every replica keeps HW ≤ LEO, and agrees with the leader below its own HW', () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { context } = run(seed, topology, steps);
          for (const topic of context.state.topics.values()) {
            for (const p of topic.partitions) {
              const leader = p.leader === null ? undefined : p.replicaLogs.get(p.leader);
              for (const replica of p.replicaLogs.values()) {
                expect(replica.highWatermark).toBeLessThanOrEqual(replica.logEndOffset);
                expect(replica.log.length).toBe(replica.logEndOffset - replica.logStartOffset);
                if (leader === undefined || replica === leader) continue;
                for (let o = replica.logStartOffset; o < replica.highWatermark; o++) {
                  expect(replica.log[o]?.traceId).toBe(leader.log[o]?.traceId);
                }
              }
            }
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });

    it('a running follower of a running leader keeps fetching (no stuck fetcher)', () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { engine, context } = run(seed, topology, steps);
          // Stop new records, then give fetchers time: a backoff (1 s) plus a long poll (500 ms).
          // (and consumers: their commits keep writing to __consumer_offsets)
          for (const id of [...context.state.producers.keys(), ...context.state.consumers.keys()]) {
            engine.dispatch({ type: 'RemoveComponent', id });
          }
          engine.runUntil(engine.now() + 2000);
          for (const topic of context.state.topics.values()) {
            for (const p of topic.partitions) {
              if (p.leader === null || !alive(context, p.leader)) continue;
              const leader = p.replicaLogs.get(p.leader);
              for (const [broker, replica] of p.replicaLogs) {
                if (broker === p.leader || !alive(context, broker)) continue;
                expect({ broker, leo: replica.logEndOffset }).toEqual({
                  broker,
                  leo: leader?.logEndOffset,
                });
              }
            }
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });

    it('leader ∈ ISR (or none), ISR ⊆ replicas, ELR disjoint from the ISR; epochs only increase', () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { context, events } = run(seed, topology, steps);
          for (const topic of context.state.topics.values()) {
            for (const p of topic.partitions) {
              for (const b of p.isr) expect(p.replicas).toContain(b);
              for (const b of p.elr) {
                expect(p.replicas).toContain(b);
                expect(p.isr).not.toContain(b);
              }
              if (p.leader !== null) expect(p.isr).toContain(p.leader);
            }
          }
          const epochs = new Map<string, number>();
          for (const e of events) {
            if (e.type === 'ComponentRemoved' && e.component.kind === 'topic') {
              for (const k of [...epochs.keys()])
                if (k.startsWith(`${e.component.id}#`)) epochs.delete(k);
            }
            if (e.type === 'LeaderElected' || e.type === 'IsrShrunk' || e.type === 'IsrExpanded') {
              const key = `${e.topic}#${String(e.partition)}`;
              const epoch = e.type === 'LeaderElected' ? e.leaderEpoch : e.partitionEpoch;
              const k2 = `${key}:${e.type === 'LeaderElected' ? 'leader' : 'partition'}`;
              expect(epoch).toBeGreaterThan(epochs.get(k2) ?? -1);
              epochs.set(k2, epoch);
            }
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });

    // A last-known-leader election (KIP-966, M4 follow-up D1) is flagged unclean like Kafka does,
    // but it doesn't count here: the leader held the only copy, so it must lose nothing committed.
    // This pins the UI's "nothing lost" text. (Acked acks=1 records that only an older leader had
    // can still be truncated in that epoch, as in real Kafka; that loss isn't this election's.)
    it('a committed record is never truncated unless an unclean election of an out-of-sync replica happened in its partition', () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { events } = run(seed, topology, steps);
          const unclean = new Set<string>();
          for (const e of events) {
            if (e.type === 'ComponentRemoved' && e.component.kind === 'topic') {
              for (const k of [...unclean])
                if (k.startsWith(`${e.component.id}#`)) unclean.delete(k);
            }
            if (e.type === 'LeaderElected' && e.lastKnownLeader) {
              expect({ at: e.t, reason: e.reason }).toEqual({ at: e.t, reason: 'unclean' });
            }
            if (e.type === 'LeaderElected' && e.reason === 'unclean' && !e.lastKnownLeader) {
              unclean.add(`${e.topic}#${String(e.partition)}`);
            }
            if (
              e.type === 'RecordsTruncated' &&
              !unclean.has(`${e.topic}#${String(e.partition)}`)
            ) {
              expect({ at: e.t, committed: e.committed }).toEqual({ at: e.t, committed: 0 });
            }
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });

    // Random scenarios almost never produce a last-known-leader election (0 in 300 samples), so
    // this one builds it on purpose: crash the followers, then the leader, bring the leader back
    // (KIP-966 elects it when no ELR member is left), then the followers. Unclean election is off,
    // so every unclean election must be a last-known-leader one, and none may lose a committed or
    // acknowledged acks=all record (M4 follow-up D1: the UI says "nothing lost").
    it('a last-known-leader election loses no committed or acked acks=all record', () => {
      let withLastKnownLeader = 0;
      const scenarioArb = fc.record({
        brokers: fc.integer({ min: 1, max: 3 }),
        minIsrPick: fc.nat(),
        acks: fc.constantFrom('0' as const, '1' as const, 'all' as const),
        rate: fc.integer({ min: 1, max: 20 }),
        order: fc.shuffledSubarray([0, 1], { minLength: 2, maxLength: 2 }),
        gaps: fc.array(fc.integer({ min: 0, max: 4000 }), { minLength: 6, maxLength: 6 }),
      });
      fc.assert(
        fc.property(seedArb, scenarioArb, (seed, sc) => {
          const n = sc.brokers;
          const topology: TopologyInput = {
            brokers: Array.from({ length: n }, (_, i) => ({
              kind: 'broker' as const,
              id: `b${String(i + 1)}`,
            })),
            topics: [
              {
                kind: 'topic',
                id: 'orders',
                partitions: 1,
                replicationFactor: n,
                minInsyncReplicas: 1 + (sc.minIsrPick % n),
                uncleanLeaderElectionEnable: false,
              },
            ],
            producers: [{ kind: 'producer', id: 'p1', ratePerSec: sc.rate, acks: sc.acks }],
            connections: [{ from: 'p1', to: 'orders' }],
          };
          const { engine, context } = createEngineInternal({ seed, topology });
          const events: EngineEvent[] = [];
          engine.on((e) => events.push(e));
          const gap = (i: number) => {
            // past broker.session.timeout.ms (9 s) so each crash is fenced before the next step
            engine.runUntil(engine.now() + 10_000 + (sc.gaps[i] ?? 0));
          };
          const send = (command: Command) => engine.dispatch(command);
          gap(0);
          const leader = context.state.topics.get('orders')?.partitions[0]?.leader ?? 'b1';
          const others = Array.from({ length: n }, (_, i) => `b${String(i + 1)}`).filter(
            (b) => b !== leader,
          );
          const followers = sc.order.map((i) => others[i]).filter((b) => b !== undefined);
          for (const [i, f] of followers.entries()) {
            send({ type: 'KillBroker', id: f });
            gap(1 + i);
          }
          send({ type: 'KillBroker', id: leader });
          gap(3);
          send({ type: 'RestartBroker', id: leader });
          gap(4);
          for (const f of followers) send({ type: 'RestartBroker', id: f });
          gap(5);
          engine.runUntil(engine.now() + 20_000);

          const acksAll = new Set<string>();
          const acked = new Set<string>();
          let hit = false;
          for (const e of events) {
            if (e.type === 'RecordProduced' && e.acks === 'all') acksAll.add(e.traceId);
            if (e.type === 'ProduceAcked') acked.add(e.traceId);
            if (e.type === 'LeaderElected' && e.reason === 'unclean') {
              expect({ at: e.t, lastKnownLeader: e.lastKnownLeader }).toEqual({
                at: e.t,
                lastKnownLeader: true,
              });
            }
            if (e.type === 'LeaderElected' && e.lastKnownLeader) hit = true;
            if (e.type === 'RecordsTruncated') {
              expect({ at: e.t, committed: e.committed }).toEqual({ at: e.t, committed: 0 });
              for (const t of e.traceIds) {
                expect({ lost: t, acked: acked.has(t) && acksAll.has(t) }).toEqual({
                  lost: t,
                  acked: false,
                });
              }
            }
          }
          if (hit) withLastKnownLeader++;
        }),
        { numRuns: NUM_RUNS },
      );
      // the scenario must actually reach the case it is about
      expect(withLastKnownLeader).toBeGreaterThan(0);
    });

    it('acks=all with effective minISR ≥ 2 and no unclean election never loses an acknowledged record', () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { context, events } = run(seed, topology, steps);
          const acksAll = new Set<string>();
          const acked = new Set<string>();
          for (const e of events) {
            if (e.type === 'RecordProduced' && e.acks === 'all') acksAll.add(e.traceId);
            if (e.type === 'ProduceAcked') acked.add(e.traceId);
            if (e.type !== 'RecordsTruncated') continue;
            const topic = context.state.topics.get(e.topic);
            const p = topic?.partitions[e.partition];
            if (
              topic === undefined ||
              p === undefined ||
              topic.config.uncleanLeaderElectionEnable
            ) {
              continue;
            }
            if (Math.min(topic.config.minInsyncReplicas, p.replicas.length) < 2) continue;
            for (const t of e.traceIds) {
              expect({ lost: t, acked: acked.has(t) && acksAll.has(t) }).toEqual({
                lost: t,
                acked: false,
              });
            }
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });
  },
);
