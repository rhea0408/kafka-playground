import { useState, type KeyboardEvent } from 'react';
import { INPUT } from './controls.tsx';
import { errorText, type FieldError } from './FieldFrame.tsx';
import { checkField, fromInputText } from './field-value.ts';

/** FR-4.7: shown under the stepper (wireframe 05b), plus when new partitions reach clients (T2-2). */
export const PARTITION_WARNING =
  'Adding partitions changes which partition each key goes to. Partitions can’t be reduced later.';
export const PARTITION_METADATA_NOTE =
  'Producers and consumers only notice new partitions at their next metadata refresh (metadata.max.age.ms, up to 5 min by default).';

const STEP_BUTTON =
  'flex h-[30px] w-8 shrink-0 items-center justify-center rounded-sm border border-border bg-surface text-[15px] ' +
  'hover:bg-muted disabled:cursor-not-allowed disabled:bg-muted disabled:text-status/50 ' +
  'focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-primary';

export interface PartitionsStepperProps {
  readonly inputId: string;
  readonly topicId: string;
  /** The topic's partition count now. */
  readonly current: number;
  readonly describedBy: string;
  readonly commit: (value: number) => Promise<string | null>;
}

/**
 * Partitions stepper (wireframe 05b). Adding partitions can't be undone, so unlike the other
 * fields it doesn't save on blur: − / + (or ArrowDown / ArrowUp in the box) pick a new count, and
 * the "Add" button (or Enter) applies it (M2-decisions T10). − is disabled at the current count
 * because Kafka never removes partitions; a lower number typed into the box is sent anyway, so the
 * engine's refusal (the KRaft controller's reason) is shown here.
 */
export function PartitionsStepper(props: PartitionsStepperProps) {
  const { inputId, topicId, current, describedBy, commit } = props;
  const [draft, setDraft] = useState<string | null>(null);
  const [error, setError] = useState<FieldError | null>(null);
  const [pending, setPending] = useState(false);

  const text = draft ?? String(current);
  const parsed = fromInputText(text, { type: 'partitions' });
  const checked = parsed.ok ? checkField('topic', 'partitions', parsed.value) : parsed;
  const target = checked.ok && typeof checked.value === 'number' ? checked.value : null;
  const minusDisabled = target === null || target <= current;
  const canApply = target !== null && target !== current && !pending;
  const minusReasonId = `${inputId}-min-reason`;
  const errorId = `${inputId}-error`;
  const shownError: FieldError | null = checked.ok
    ? error
    : { source: 'schema', message: checked.error };

  const set = (n: number): void => {
    setDraft(n === current ? null : String(n));
    setError(null);
  };

  const apply = async (): Promise<void> => {
    if (!canApply) return;
    setPending(true);
    const reason = await commit(target);
    setPending(false);
    if (reason === null) {
      setDraft(null);
      setError(null);
    } else {
      setError({ source: 'engine', message: reason });
    }
  };

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>): void => {
    switch (event.key) {
      case 'ArrowUp':
        event.preventDefault();
        set((target ?? current) + 1);
        break;
      case 'ArrowDown':
        event.preventDefault();
        if (target !== null && target > current) set(target - 1);
        break;
      case 'Enter':
        event.preventDefault();
        void apply();
        break;
      case 'Escape':
        setDraft(null);
        setError(null);
        break;
      default:
        break;
    }
  };

  const delta = target === null ? 0 : target - current;
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center gap-1.5">
        <button
          type="button"
          className={STEP_BUTTON}
          aria-label="One partition fewer"
          aria-describedby={minusReasonId}
          disabled={minusDisabled}
          onClick={() => {
            if (target !== null) set(target - 1);
          }}
        >
          −
        </button>
        <input
          id={inputId}
          type="text"
          inputMode="numeric"
          role="spinbutton"
          aria-valuemin={current}
          aria-valuenow={target ?? undefined}
          aria-describedby={`${describedBy} ${minusReasonId}${shownError === null ? '' : ` ${errorId}`}`}
          aria-invalid={shownError !== null}
          className={`${INPUT} w-[60px]! text-center`}
          value={text}
          onChange={(event) => {
            setDraft(event.target.value);
            setError(null);
          }}
          onKeyDown={onKeyDown}
        />
        <button
          type="button"
          className={STEP_BUTTON}
          aria-label="One partition more"
          onClick={() => {
            set((target ?? current) + 1);
          }}
        >
          +
        </button>
        <button
          type="button"
          className="ml-auto h-[30px] whitespace-nowrap rounded-sm bg-primary px-3 text-[12px] font-medium text-surface hover:brightness-95 disabled:cursor-not-allowed disabled:bg-muted disabled:text-status/60 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
          disabled={!canApply}
          onClick={() => void apply()}
        >
          {delta > 0
            ? `Add ${String(delta)} partition${delta === 1 ? '' : 's'}`
            : delta < 0
              ? 'Apply'
              : 'Add partitions'}
        </button>
      </div>
      <p id={minusReasonId} className="text-[11px] leading-snug text-status/70">
        {target !== null && target > current
          ? `${topicId} has ${String(current)} now; applying makes it ${String(target)}.`
          : `Can’t go below ${String(current)}: Kafka never removes partitions from a topic.`}
      </p>
      {shownError !== null && (
        <p
          id={errorId}
          className="text-[11px] leading-snug text-danger-text"
          data-error-source={shownError.source}
        >
          {errorText(shownError)}
        </p>
      )}
      <div
        role="note"
        className="rounded border border-dashed border-danger bg-danger-bg px-2 py-1.5 text-[11px] leading-snug text-danger-text"
      >
        <p>{PARTITION_WARNING}</p>
        <p className="mt-1">{PARTITION_METADATA_NOTE}</p>
      </div>
    </div>
  );
}
