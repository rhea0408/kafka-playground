import { useState } from 'react';
import { useActions } from '../actions/actions-context.tsx';
import { INPUT } from './controls.tsx';
import { checkField, fromInputText } from './field-value.ts';

/** Shown with the control: Kafka can't change RF in place (M4-T11). */
export const RF_RECREATE_NOTE =
  'Kafka changes a topic’s replication factor by reassigning partitions (not modelled yet), so this re-creates the topic: its records and committed offsets are deleted.';

const BUTTON =
  'h-[30px] shrink-0 rounded-sm border border-border bg-surface px-2 text-xs hover:bg-muted ' +
  'disabled:cursor-not-allowed disabled:bg-muted disabled:text-status/50 ' +
  'focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-primary';

export interface ReplicationFactorFieldProps {
  readonly inputId: string;
  readonly topicId: string;
  readonly current: number;
  readonly minInsyncReplicas: number;
  readonly describedBy: string;
}

/**
 * Replication factor of a topic (M4-T11): a number and a "Re-create topic" button, with the
 * warning that the topic's data goes. Also warns when min.insync.replicas is above it: Kafka then
 * applies the replication factor as the effective minimum (M4-decisions T3-4).
 */
export function ReplicationFactorField(props: ReplicationFactorFieldProps) {
  const { inputId, topicId, current, minInsyncReplicas, describedBy } = props;
  const actions = useActions();
  const [draft, setDraft] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const text = draft ?? String(current);
  const parsed = fromInputText(text, { type: 'replicationFactor' });
  const checked = parsed.ok ? checkField('topic', 'replicationFactor', parsed.value) : parsed;
  const target = checked.ok && typeof checked.value === 'number' ? checked.value : null;
  const noteId = `${inputId}-note`;
  return (
    <div className="flex flex-col gap-1">
      <div className="flex items-center gap-1.5">
        <input
          id={inputId}
          className={`${INPUT} w-16 font-mono`}
          inputMode="numeric"
          value={text}
          aria-describedby={`${describedBy} ${noteId}`}
          aria-invalid={!checked.ok}
          onChange={(event) => {
            setDraft(event.target.value);
          }}
        />
        <button
          type="button"
          className={BUTTON}
          disabled={target === null || target === current || pending}
          onClick={() => {
            if (target === null) return;
            setPending(true);
            void actions.recreateTopic(topicId, target).finally(() => {
              setPending(false);
              setDraft(null);
            });
          }}
        >
          Re-create topic with RF {target ?? '?'}
        </button>
      </div>
      {!checked.ok && <p className="text-[11px] text-danger-text">{checked.error}</p>}
      <p id={noteId} className="text-[11px] leading-snug text-status/70">
        {RF_RECREATE_NOTE}
      </p>
      {minInsyncReplicas > current && (
        <p
          className="rounded border border-dashed border-danger bg-danger-bg px-2 py-1 text-[11px] text-danger-text"
          data-slot="min-isr-warning"
        >
          ⚠ min.insync.replicas ({minInsyncReplicas}) is above the replication factor ({current}).
          Kafka applies {current}: acks=all needs every replica in sync.
        </p>
      )}
    </div>
  );
}
