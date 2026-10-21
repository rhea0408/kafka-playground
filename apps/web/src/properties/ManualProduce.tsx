import { partitionName, type EngineEvent } from '@kp/engine';
import { useState, type FormEvent } from 'react';
import { useActions } from '../actions/actions-context.tsx';
import { useSimStore } from '../stores/sim-store.ts';
import { useTopologyStore } from '../stores/topology-store.ts';
import { INPUT } from './controls.tsx';

/** Longest key and value the form accepts (a game limit to keep rows readable, not Kafka's). */
export const MAX_KEY_LENGTH = 64;
export const MAX_VALUE_LENGTH = 256;

export interface ManualRecordInput {
  readonly key: string | null;
  readonly value: string;
}

/**
 * Checks the form. An empty key box means a **null key** (ProducerRecord(topic, value)); any other
 * text, spaces included, is the key exactly as typed. An empty value is allowed: Kafka accepts
 * empty values.
 */
export function parseManualRecord(
  keyText: string,
  valueText: string,
): { ok: true; value: ManualRecordInput } | { ok: false; error: string } {
  if (keyText.length > MAX_KEY_LENGTH) {
    return { ok: false, error: `Keep the key to ${String(MAX_KEY_LENGTH)} characters.` };
  }
  if (valueText.length > MAX_VALUE_LENGTH) {
    return { ok: false, error: `Keep the value to ${String(MAX_VALUE_LENGTH)} characters.` };
  }
  return { ok: true, value: { key: keyText === '' ? null : keyText, value: valueText } };
}

/** What happened to a sent record so far, per topic: chosen partition, then its offset. */
export interface Delivery {
  readonly topic: string;
  readonly partition: number;
  readonly offset: number | null;
}

/**
 * The deliveries of the record sent after event `sinceSeq`: its RecordProduced events (one per
 * connected topic; the partitioner's choice) and their RecordAppended (the offset the leader gave
 * it), matched by trace id.
 */
export function findDeliveries(
  events: readonly EngineEvent[],
  producerId: string,
  sent: ManualRecordInput,
  sinceSeq: number,
): Delivery[] {
  const out: Delivery[] = [];
  const traces = new Map<string, number>();
  for (const e of events) {
    if (e.seq <= sinceSeq) continue;
    if (
      e.type === 'RecordProduced' &&
      e.producerId === producerId &&
      e.key === sent.key &&
      e.value === sent.value &&
      !out.some((d) => d.topic === e.topic)
    ) {
      traces.set(e.traceId, out.length);
      out.push({ topic: e.topic, partition: e.partition, offset: null });
    } else if (e.type === 'RecordAppended') {
      const index = traces.get(e.traceId);
      const delivery = index === undefined ? undefined : out[index];
      if (index !== undefined && delivery !== undefined) {
        out[index] = { ...delivery, offset: e.offset };
      }
    }
  }
  return out;
}

type Status =
  | { readonly type: 'idle' }
  | { readonly type: 'invalid'; readonly message: string }
  | { readonly type: 'rejected'; readonly message: string }
  | { readonly type: 'sent'; readonly record: ManualRecordInput; readonly sinceSeq: number };

const LABEL = 'text-[13px] font-semibold';

/**
 * Manual "produce message" form (FR-6.5) on a producer's properties: one record with the key and
 * value typed here, sent through `ProduceManual` to every topic the producer is connected to.
 * The engine's command has no explicit partition or headers (M2-decisions T11), so the form has
 * neither: the producer's partitioner chooses, as for `send(new ProducerRecord(topic, key, value))`.
 */
export function ManualProduce({ producerId }: { producerId: string }) {
  const actions = useActions();
  const connected = useTopologyStore((s) => s.connections.some((c) => c.from === producerId));
  const [keyText, setKeyText] = useState('');
  const [valueText, setValueText] = useState('hello');
  const [status, setStatus] = useState<Status>({ type: 'idle' });
  const [busy, setBusy] = useState(false);

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    const parsed = parseManualRecord(keyText, valueText);
    if (!parsed.ok) {
      setStatus({ type: 'invalid', message: parsed.error });
      return;
    }
    const sinceSeq = useSimStore.getState().events.at(-1)?.seq ?? -1;
    setBusy(true);
    void actions
      .produceManual(producerId, parsed.value.key, parsed.value.value)
      .then((result) => {
        setStatus(
          result.ok
            ? { type: 'sent', record: parsed.value, sinceSeq }
            : { type: 'rejected', message: result.reason },
        );
      })
      .finally(() => {
        setBusy(false);
      });
  };

  const invalid = status.type === 'invalid';
  return (
    <section aria-labelledby="mp-title" className="flex flex-col gap-2">
      <h3 id="mp-title" className="mt-1 text-[11px] font-bold text-status/70">
        PRODUCE A RECORD
      </h3>
      <form className="flex w-[328px] flex-col gap-2" onSubmit={submit} noValidate>
        <div className="flex flex-col gap-1">
          <label htmlFor="mp-key" className={LABEL}>
            Record key <span className="font-normal text-status/70">(optional)</span>
          </label>
          <input
            id="mp-key"
            className={`${INPUT} font-mono`}
            value={keyText}
            placeholder="empty = null key"
            aria-describedby="mp-key-help"
            aria-invalid={invalid && keyText.length > MAX_KEY_LENGTH}
            onChange={(e) => {
              setKeyText(e.target.value);
              if (invalid) setStatus({ type: 'idle' });
            }}
          />
          <p id="mp-key-help" className="text-[11px] leading-snug text-status/70">
            {keyText === ''
              ? 'No key: the sticky partitioner picks a partition and keeps it for a batch (KIP-480, KIP-794).'
              : 'Same key, same partition: murmur2(key) % partitions.'}
          </p>
        </div>
        <div className="flex flex-col gap-1">
          <label htmlFor="mp-value" className={LABEL}>
            Record value
          </label>
          <input
            id="mp-value"
            className={`${INPUT} font-mono`}
            value={valueText}
            aria-invalid={invalid && valueText.length > MAX_VALUE_LENGTH}
            aria-describedby={status.type === 'idle' ? undefined : 'mp-status'}
            onChange={(e) => {
              setValueText(e.target.value);
              if (invalid) setStatus({ type: 'idle' });
            }}
          />
        </div>
        <button
          type="submit"
          disabled={!connected || busy}
          aria-describedby={connected ? undefined : 'mp-unconnected'}
          className="h-8 rounded-md bg-primary text-[13px] font-semibold text-surface hover:brightness-95 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary disabled:cursor-not-allowed disabled:bg-muted disabled:text-status/70"
        >
          Send record
        </button>
        {!connected && (
          <p id="mp-unconnected" className="text-[11px] text-status/70">
            Connect this producer to a topic first.
          </p>
        )}
        <div id="mp-status" aria-live="polite" className="text-[12px]">
          <StatusLine producerId={producerId} status={status} />
        </div>
      </form>
    </section>
  );
}

function StatusLine({ producerId, status }: { producerId: string; status: Status }) {
  switch (status.type) {
    case 'idle':
      return null;
    case 'invalid':
      return <p className="text-danger-text">{status.message}</p>;
    case 'rejected':
      return <p className="text-danger-text">Not sent: {status.message}</p>;
    case 'sent':
      return <SentLine producerId={producerId} status={status} />;
  }
}

function SentLine({
  producerId,
  status,
}: {
  producerId: string;
  status: Extract<Status, { type: 'sent' }>;
}) {
  const events = useSimStore((s) => s.events);
  const deliveries = findDeliveries(events, producerId, status.record, status.sinceSeq);
  const key = status.record.key === null ? 'null key' : `key ${status.record.key}`;
  if (deliveries.length === 0) return <p>Sent ({key}).</p>;
  return (
    <ul data-testid="mp-deliveries">
      {deliveries.map((d) => (
        <li key={d.topic}>
          Sent ({key}) → <span className="font-mono">{partitionName(d.topic, d.partition)}</span>
          {d.offset === null
            ? ': on its way to the leader (press Play or Step if paused).'
            : `, offset ${String(d.offset)}.`}
        </li>
      ))}
    </ul>
  );
}
