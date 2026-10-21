/**
 * Headless demo (M1-T10, extended in M2-T2): producer (keys A/B/C, 2 msg/s) → topic "orders"
 * (3 partitions) → consumer, for 5 sim seconds. Halfway through, UpdateConfig raises the rate to
 * 4 msg/s and grows the topic to 4 partitions. Prints the event log, then a snapshot of
 * partitions and lag.
 *
 *   pnpm --filter @kp/engine demo                 # seed 42, 5000 ms
 *   pnpm --filter @kp/engine demo -- --seed 7 --until 12000
 */

import { createEngine, formatEvent, type EngineSnapshot } from '../src/index.ts';

function option(name: string, fallback: number): number {
  const i = process.argv.indexOf(`--${name}`);
  const raw = i === -1 ? undefined : process.argv[i + 1];
  const value = raw === undefined ? fallback : Number(raw);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`--${name} must be a non-negative integer, got ${String(raw)}`);
  }
  return value;
}

const seed = option('seed', 42);
const until = option('until', 5000);
const half = Math.floor(until / 2);

const engine = createEngine({
  seed,
  topology: {
    brokers: [{ kind: 'broker', id: 'b1' }],
    topics: [{ kind: 'topic', id: 'orders', partitions: 3 }],
    producers: [
      {
        kind: 'producer',
        id: 'p1',
        ratePerSec: 2,
        keyStrategy: { type: 'random', keys: ['A', 'B', 'C'] },
      },
    ],
    consumers: [{ kind: 'consumer', id: 'c1', autoOffsetReset: 'earliest' }],
    connections: [
      { from: 'p1', to: 'orders' },
      { from: 'orders', to: 'c1' },
    ],
  },
});

const out: string[] = [];
out.push(`Kafka Playground engine demo · seed ${String(seed)} · ${String(until)} ms of sim time`);
out.push('p1 (keys A/B/C, 2 msg/s, acks=all) → orders (3 partitions on b1) → c1 (group c1-group)');
out.push(`At ${String(half)} ms: p1 → 4 msg/s and orders → 4 partitions (UpdateConfig)`);
out.push('');
engine.on((e) => out.push(formatEvent(e)));
engine.runUntil(half);
for (const command of [
  { type: 'UpdateConfig', id: 'p1', patch: { ratePerSec: 4 } },
  { type: 'UpdateConfig', id: 'orders', patch: { partitions: 4 } },
] as const) {
  const result = engine.dispatch(command);
  if (!result.ok) throw new Error(result.reason);
}
engine.runUntil(until);
out.push('', ...snapshotTables(engine.getSnapshot()));
console.log(out.join('\n'));

function snapshotTables(snap: EngineSnapshot): string[] {
  const lines: string[] = [];
  for (const topic of snap.topics) {
    lines.push(`Topic ${topic.config.id} at ${String(snap.t)} ms`);
    lines.push(
      ...table(
        ['partition', 'leader', 'LSO', 'LEO', 'HW', 'keys in log (oldest → newest)'],
        topic.partitions.map((p) => [
          `${topic.config.id}-${String(p.index)}`,
          p.leader,
          String(p.logStartOffset),
          String(p.logEndOffset),
          String(p.highWatermark),
          p.records.map((r) => r.key ?? '∅').join(' ') || '(empty)',
        ]),
      ),
    );
    const empty = topic.partitions.filter((p) => p.logEndOffset === 0);
    if (empty.length > 0 && empty.length < topic.partitions.length) {
      lines.push(
        `Note: ${String(empty.length)} of ${String(topic.partitions.length)} partitions got no records. Each key goes to`,
        `toPositive(murmur2(key)) % partitions, the Java client's rule, and with only a few keys`,
        `the hashes often collide: here A, B and C all map to partition 1 of 3.`,
      );
    }
    if (topic.partitions.length > 3) {
      lines.push(
        `Partition 3 was added at ${String(half)} ms but stays empty: clients only see new partitions`,
        `after their next metadata refresh (metadata.max.age.ms, 5 minutes by default).`,
      );
    }
    lines.push('');
  }
  for (const consumer of snap.consumers) {
    lines.push(
      `Consumer ${consumer.config.id} (group ${consumer.config.groupId}, ${consumer.status}) · consumed ${String(consumer.consumed)} · total lag ${String(consumer.totalLag)}`,
    );
    lines.push(
      ...table(
        ['partition', 'position', 'committed', 'HW', 'lag (HW − committed)'],
        consumer.partitions.map((p) => [
          `${p.topic}-${String(p.partition)}`,
          p.position === null ? '-' : String(p.position),
          p.committed === null ? '-' : String(p.committed),
          String(p.highWatermark),
          p.lag === null ? '-' : String(p.lag),
        ]),
      ),
    );
  }
  for (const producer of snap.producers) {
    const { sent, acked, failed } = producer.stats;
    lines.push(
      '',
      `Producer ${producer.config.id} · sent ${String(sent)} · acked ${String(acked)} · failed ${String(failed)}`,
    );
  }
  return lines;
}

function table(headers: string[], rows: string[][]): string[] {
  const widths = headers.map((h, i) => Math.max(h.length, ...rows.map((r) => (r[i] ?? '').length)));
  const line = (cells: string[]) =>
    cells
      .map((c, i) => c.padEnd(widths[i] ?? 0))
      .join('  ')
      .trimEnd();
  return [line(headers), line(widths.map((w) => '─'.repeat(w))), ...rows.map(line)];
}
