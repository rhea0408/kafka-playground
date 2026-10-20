import type { ComponentKind } from '@kp/schema';
import type { KeyboardEvent } from 'react';
import type { Control } from './field-meta.ts';
import type { FieldError } from './FieldFrame.tsx';
import { parseFieldText, toInputText } from './field-value.ts';

/**
 * Sends one field's new value. Resolves to null when the engine accepted it, or to the engine's
 * reason when it refused.
 */
export type Commit = (value: unknown) => Promise<string | null>;

export const INPUT =
  'h-[30px] w-full rounded-sm border border-border bg-surface px-2 text-[13px] text-status ' +
  'focus-visible:outline-2 focus-visible:-outline-offset-1 focus-visible:outline-primary ' +
  'aria-[invalid=true]:border-danger aria-[invalid=true]:border-[1.5px] ' +
  'read-only:bg-muted read-only:text-status/80 disabled:bg-muted disabled:text-status/70';

/** Draft text and error of one text box; null draft = show the committed value. */
export interface DraftState {
  readonly draft: string | null;
  readonly error: FieldError | null;
}

export interface TextFieldProps {
  readonly inputId: string;
  readonly kind: ComponentKind;
  readonly field: string;
  readonly control: Control;
  readonly value: unknown;
  readonly describedBy: string;
  readonly state: DraftState;
  readonly setState: (state: DraftState) => void;
  readonly commit: Commit;
}

/**
 * A text box for numbers or text. Validates with the field's Zod schema on every keystroke
 * (inline error), and saves on blur or Enter when valid (M2-decisions T10: no Save button, as in
 * wireframe 05b). Escape puts back the committed value. A refused save keeps what was typed and
 * shows the engine's reason under the field.
 */
export function TextField(props: TextFieldProps) {
  const { inputId, kind, field, control, value, describedBy, state, setState, commit } = props;
  const committedText = toInputText(value);
  const text = state.draft ?? committedText;
  const unit = control.type === 'int' || control.type === 'number' ? control.unit : undefined;

  const save = async (): Promise<void> => {
    if (state.draft === null || state.draft === committedText) {
      setState({ draft: null, error: null });
      return;
    }
    const checked = parseFieldText(kind, field, state.draft, control);
    if (!checked.ok) {
      setState({ draft: state.draft, error: { source: 'schema', message: checked.error } });
      return;
    }
    const draft = state.draft;
    const reason = await commit(checked.value);
    setState(
      reason === null
        ? { draft: null, error: null }
        : { draft, error: { source: 'engine', message: reason } },
    );
  };

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === 'Enter') {
      event.preventDefault();
      void save();
    } else if (event.key === 'Escape') {
      setState({ draft: null, error: null });
    }
  };

  return (
    <div className="flex items-center gap-2">
      <input
        id={inputId}
        type="text"
        inputMode={
          control.type === 'int' ? 'numeric' : control.type === 'number' ? 'decimal' : 'text'
        }
        className={INPUT}
        value={text}
        aria-describedby={describedBy}
        aria-invalid={state.error !== null}
        onChange={(event) => {
          const draft = event.target.value;
          const checked = parseFieldText(kind, field, draft, control);
          setState({
            draft,
            error: checked.ok ? null : { source: 'schema', message: checked.error },
          });
        }}
        onBlur={() => void save()}
        onKeyDown={onKeyDown}
      />
      {unit !== undefined && (
        <span className="w-12 shrink-0 text-[12px] text-status/70" aria-hidden="true">
          {unit}
        </span>
      )}
    </div>
  );
}

export interface ChoiceFieldProps {
  readonly inputId: string;
  readonly value: unknown;
  readonly describedBy: string;
  readonly invalid: boolean;
  readonly commit: (value: unknown) => void;
}

/** A select for enum fields (acks, auto.offset.reset). Saves on change. */
export function EnumField(props: ChoiceFieldProps & { readonly options: readonly string[] }) {
  const { inputId, value, describedBy, invalid, commit, options } = props;
  return (
    <select
      id={inputId}
      className={INPUT}
      value={toInputText(value)}
      aria-describedby={describedBy}
      aria-invalid={invalid}
      onChange={(event) => {
        commit(event.target.value);
      }}
    >
      {options.map((o) => (
        <option key={o} value={o}>
          {o}
        </option>
      ))}
    </select>
  );
}

/** A checkbox for boolean fields, with "true" / "false" written next to it. Saves on change. */
export function BooleanField(props: ChoiceFieldProps) {
  const { inputId, value, describedBy, invalid, commit } = props;
  const checked = value === true;
  return (
    <div className="flex h-[30px] items-center gap-2 text-[13px]">
      <input
        id={inputId}
        type="checkbox"
        className="size-4 accent-primary focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
        checked={checked}
        aria-describedby={describedBy}
        aria-invalid={invalid}
        onChange={(event) => {
          commit(event.target.checked);
        }}
      />
      <span className="font-mono text-[12px]" aria-hidden="true">
        {checked ? 'true' : 'false'}
      </span>
    </div>
  );
}

/**
 * A field that can't be changed: read-only (immutable in M2; focusable so the value and reason
 * can be read) or disabled (not simulated yet, shows the real Kafka default).
 */
export function FixedField(props: {
  readonly inputId: string;
  readonly text: string;
  readonly describedBy: string;
  readonly disabled: boolean;
}) {
  const { inputId, text, describedBy, disabled } = props;
  return (
    <input
      id={inputId}
      type="text"
      className={`${INPUT} ${disabled ? 'cursor-not-allowed' : ''}`}
      value={text}
      readOnly
      disabled={disabled}
      aria-describedby={describedBy}
    />
  );
}
