import { formatCount, LAG_DANGER_THRESHOLD } from '../canvas/live/format.ts';
import { useSimStore, type SimState } from '../stores/sim-store.ts';
import { useTopologyStore } from '../stores/topology-store.ts';

/** Sum of the log end offsets of a topic's partitions (records ever written; no retention yet). */
export const selectLeoTotal =
  (topicId: string) =>
  (s: SimState): number | null => {
    const topic = s.snapshot?.topics.find((t) => t.config.id === topicId);
    return topic === undefined
      ? null
      : topic.partitions.reduce((sum, p) => sum + p.logEndOffset, 0);
  };

/**
 * A consumer's lag on one topic: the sum over that topic's partitions with a committed offset
 * (kafka-consumer-groups.sh prints "-" for the others, so they're left out, as on the node).
 */
export const selectTopicLag =
  (consumerId: string, topicId: string) =>
  (s: SimState): number | null => {
    const consumer = s.snapshot?.consumers.find((c) => c.config.id === consumerId);
    let lag: number | null = null;
    for (const p of consumer?.partitions ?? []) {
      if (p.topic === topicId && p.lag !== null) lag = (lag ?? 0) + p.lag;
    }
    return lag;
  };

function StatTile({ label, value, title }: { label: string; value: string; title?: string }) {
  return (
    <div
      className="flex h-12 w-[104px] flex-col justify-center rounded-md bg-muted px-2"
      title={title}
    >
      <div className="text-[10px] text-status/70">{label}</div>
      <div className="font-mono text-[13px] font-semibold">{value}</div>
    </div>
  );
}

/** Topic stat tiles, 104 × 48 (ui-spec, wireframe 05b): partitions, LEO total, consumers. */
export function TopicStats({ topicId, partitions }: { topicId: string; partitions: number }) {
  const leo = useSimStore(selectLeoTotal(topicId));
  const consumers = useTopologyStore((s) => s.connections.filter((c) => c.from === topicId).length);
  return (
    <div className="grid grid-cols-[repeat(3,104px)] gap-2" aria-label="Topic stats" role="group">
      <StatTile label="Partitions" value={String(partitions)} />
      <StatTile
        label="LEO total"
        value={leo === null ? '-' : formatCount(leo)}
        title="Sum of every partition's log end offset: the records written so far."
      />
      <StatTile label="Consumers" value={String(consumers)} />
    </div>
  );
}

function ConsumerLagLine({ consumerId, topicId }: { consumerId: string; topicId: string }) {
  const lag = useSimStore(selectTopicLag(consumerId, topicId));
  const danger = lag !== null && lag > LAG_DANGER_THRESHOLD;
  return (
    <li className={danger ? 'text-danger-text' : undefined} data-danger={danger}>
      {consumerId} · lag {lag === null ? '-' : formatCount(lag)}
      {danger && ' (high)'}
    </li>
  );
}

/** "Consumed by" list (wireframe 05b): each connected consumer with its lag on this topic. */
export function ConsumedBy({ topicId }: { topicId: string }) {
  const connections = useTopologyStore((s) => s.connections);
  const consumers = connections.filter((c) => c.from === topicId).map((c) => c.to);
  return (
    <section aria-labelledby="pp-consumed-by" className="flex flex-col gap-1">
      <h3 id="pp-consumed-by" className="text-[11px] font-bold text-status/70">
        CONSUMED BY
      </h3>
      {consumers.length === 0 ? (
        <p className="text-[12px] text-status/70">No consumer reads this topic yet.</p>
      ) : (
        <ul className="font-mono text-[12px] leading-6">
          {consumers.map((id) => (
            <ConsumerLagLine key={id} consumerId={id} topicId={topicId} />
          ))}
        </ul>
      )}
    </section>
  );
}
