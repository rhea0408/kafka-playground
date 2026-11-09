/**
 * Liveness with broker failures (M4-T9). Each bound is in sim time, with its precondition:
 *
 * 1. A killed broker is fenced within broker.session.timeout.ms (9 s) of the kill: its last
 *    heartbeat reached the controller at or before the kill.
 * 2. Once every broker runs again and the cluster is left alone, every partition gets a leader,
 *    every replica catches up and rejoins the ISR, and every record a producer is still sending is
 *    settled (acknowledged or failed by delivery.timeout.ms). Budget: fencing (9 s) + registration
 *    retries (1 s each) + delivery.timeout.ms (≤ 40 s here) + a margin.
 * 3. A consumer group whose application keeps up (short batches, the M4-T0 L1–L3 precondition
 *    holds trivially) ends Stable with every running member, through coordinator moves.
 */

import type { TopologyInput } from '@kp/schema';
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import type { Command } from './commands/index.ts';
import { createEngineInternal } from './engine.ts';
import type { EngineEvent } from './events/index.ts';
import { canServe } from './protocols/controller.ts';
import { stepsArb, topologyArb, type Step } from './testing/arbitraries.ts';
import { runsFromEnv } from './testing/runs.ts';

const seedArb = fc.integer({ min: 0, max: 0xffffffff });
const NUM_RUNS = runsFromEnv('KP_INVARIANT_RUNS', 60);
const SETTLE_MS = 60_000;

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

describe(
  'liveness with broker failures (fast-check)',
  { timeout: Math.max(60_000, NUM_RUNS * 2000) },
  () => {
    it('a killed broker is fenced within broker.session.timeout.ms', () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { engine, events } = run(seed, topology, [
            ...steps,
            { kind: 'advance', ms: 10_000 },
          ]);
          for (const entry of engine.commandLog) {
            if (entry.command.type !== 'KillBroker') continue;
            const id = entry.command.id;
            // The controller's view of the broker 9 s (+1 ms for the last heartbeat's trip) later.
            const deadline = entry.t + 9001;
            const last = events
              .filter(
                (e) =>
                  (e.type === 'BrokerFenced' || e.type === 'BrokerUnfenced') &&
                  e.brokerId === id &&
                  e.t <= deadline,
              )
              .at(-1);
            // Fenced by then, unless it was restarted and unfenced again after being fenced.
            const fencedAfterKill = events.some(
              (e) =>
                e.type === 'BrokerFenced' && e.brokerId === id && e.t >= entry.t && e.t <= deadline,
            );
            const alreadyFenced = last?.type === 'BrokerFenced' && last.t < entry.t;
            expect({
              id,
              at: entry.t,
              fenced: fencedAfterKill || alreadyFenced || last?.type === 'BrokerUnfenced',
            }).toEqual({
              id,
              at: entry.t,
              fenced: true,
            });
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });

    it('with every broker back, partitions get leaders, ISRs fill up and producers settle', () => {
      fc.assert(
        fc.property(seedArb, topologyArb, stepsArb, (seed, topology, steps) => {
          const { engine, context } = run(seed, topology, steps);
          engine.runUntil(engine.now() + 10); // controlled shutdowns finish
          for (const [id, broker] of context.state.brokers) {
            if (broker.status === 'crashed' || broker.status === 'stopped') {
              engine.dispatch({ type: 'RestartBroker', id } satisfies Command);
            }
          }
          engine.runUntil(engine.now() + SETTLE_MS);
          // Stop new records and commits, then let the last ones replicate.
          // No record waits longer than delivery.timeout.ms (it is acknowledged or failed by then).
          for (const producer of context.state.producers.values()) {
            for (const pending of producer.pending.values()) {
              const age = engine.now() - pending.record.timestamp;
              expect({
                trace: pending.traceId,
                late: age > producer.config.deliveryTimeoutMs,
              }).toEqual({
                trace: pending.traceId,
                late: false,
              });
            }
          }
          for (const id of [...context.state.producers.keys(), ...context.state.consumers.keys()]) {
            engine.dispatch({ type: 'RemoveComponent', id });
          }
          engine.runUntil(engine.now() + 3000);
          for (const topic of context.state.topics.values()) {
            for (const p of topic.partitions) {
              const where = `${topic.config.id}-${String(p.index)}`;
              expect({ where, leader: p.leader !== null }).toEqual({ where, leader: true });
              expect({ where, isr: [...p.isr].sort() }).toEqual({
                where,
                isr: p.replicas.filter((b) => canServe(context, b)).sort(),
              });
              const leos = new Set([...p.replicaLogs.values()].map((r) => r.logEndOffset));
              expect({ where, leos: leos.size }).toEqual({ where, leos: 1 });
            }
          }
        }),
        { numRuns: NUM_RUNS },
      );
    });

    it('a group that keeps up ends Stable with every running member, through coordinator moves', () => {
      const brokerSteps = fc.array(
        fc.oneof(
          {
            weight: 3,
            arbitrary: fc
              .integer({ min: 0, max: 4000 })
              .map((ms): Step => ({ kind: 'advance', ms })),
          },
          {
            weight: 1,
            arbitrary: fc
              .record({
                type: fc.constantFrom(
                  'KillBroker' as const,
                  'StopBroker' as const,
                  'RestartBroker' as const,
                ),
                id: fc.constantFrom('b1', 'b2', 'b3'),
              })
              .map(({ type, id }): Step => ({ kind: 'command', command: { type, id } })),
          },
        ),
        { minLength: 3, maxLength: 20 },
      );
      fc.assert(
        fc.property(
          seedArb,
          fc.integer({ min: 2, max: 3 }),
          fc.constantFrom('range' as const, 'cooperative-sticky' as const),
          brokerSteps,
          (seed, members, assignor, steps) => {
            const topology: TopologyInput = {
              brokers: ['b1', 'b2', 'b3'].map((id) => ({
                kind: 'broker' as const,
                id,
                groupInitialRebalanceDelayMs: 0,
              })),
              topics: [
                {
                  kind: 'topic',
                  id: 'orders',
                  partitions: 4,
                  replicationFactor: 3,
                  minInsyncReplicas: 2,
                },
              ],
              producers: [{ kind: 'producer', id: 'p1', ratePerSec: 10 }],
              consumers: Array.from({ length: members }, (_, i) => ({
                kind: 'consumer' as const,
                id: `c${String(i)}`,
                groupId: 'g',
                autoOffsetReset: 'earliest' as const,
                processingTimeMs: 5,
                partitionAssignmentStrategy: [assignor],
              })),
              connections: [
                { from: 'p1', to: 'orders' },
                ...Array.from({ length: members }, (_, i) => ({
                  from: 'orders',
                  to: `c${String(i)}`,
                })),
              ],
            };
            const { engine, context } = run(seed, topology, steps);
            engine.runUntil(engine.now() + 10); // controlled shutdowns finish
            for (const [id, broker] of context.state.brokers) {
              if (broker.status === 'crashed' || broker.status === 'stopped') {
                engine.dispatch({ type: 'RestartBroker', id });
              }
            }
            engine.runUntil(engine.now() + SETTLE_MS);
            const group = context.state.groups.get('g');
            expect(group?.state).toBe('Stable');
            expect(group?.members.size).toBe(members);
            for (const consumer of context.state.consumers.values()) {
              expect({ c: consumer.config.id, state: consumer.group.state }).toEqual({
                c: consumer.config.id,
                state: 'stable',
              });
            }
          },
        ),
        { numRuns: NUM_RUNS },
      );
    });
  },
);
