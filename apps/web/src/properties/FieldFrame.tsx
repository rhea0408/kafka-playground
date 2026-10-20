import * as Tooltip from '@radix-ui/react-tooltip';
import type { ReactNode } from 'react';
import type { FieldMeta } from './field-meta.ts';
import { describeDefault } from './field-value.ts';

/** Where an error under a field came from: the field's Zod schema, or the engine's refusal. */
export interface FieldError {
  readonly source: 'schema' | 'engine';
  readonly message: string;
}

/** Engine refusals get a prefix so it's clear the change was sent and not applied. */
export function errorText(error: FieldError): string {
  return error.source === 'engine' ? `Not applied: ${error.message}` : error.message;
}

/**
 * Info tags are flat muted chips, monospace, no border, not clickable (ui-spec "Info tags",
 * CLAUDE.md UI rules). One with a tooltip is focusable so keyboard users can read it.
 */
const CHIP =
  'inline-flex h-[18px] shrink-0 items-center rounded-sm bg-muted px-1.5 font-mono text-[10px] text-status';

const FOCUS_RING =
  'focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-primary';

const TOOLTIP_CONTENT =
  'z-50 max-w-[280px] rounded bg-status px-2 py-1.5 text-[11px] leading-snug text-surface shadow';

/** A one-line tooltip on a focusable element (Radix Tooltip: hover and keyboard focus). */
export function Tip({ text, children }: { text: string; children: ReactNode }) {
  return (
    <Tooltip.Root>
      <Tooltip.Trigger asChild>{children}</Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content side="top" align="end" sideOffset={6} className={TOOLTIP_CONTENT}>
          {text}
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  );
}

/** The chip for a field's kind (Q5): none for plain Kafka fields. */
export function KindChip({ meta }: { meta: FieldMeta }) {
  switch (meta.kind) {
    case 'kafka':
      return null;
    case 'game-tuned':
      return (
        <span className={CHIP} data-chip="game-tuned">
          game-tuned
        </span>
      );
    case 'game-setting':
      return (
        <Tip text={meta.standsFor}>
          <span
            tabIndex={0}
            className={`${CHIP} cursor-help ${FOCUS_RING}`}
            data-chip="game-setting"
            aria-label={`game setting: ${meta.standsFor}`}
          >
            game setting
          </span>
        </Tip>
      );
    case 'not-modelled':
      return (
        <span className={CHIP} data-chip="not-modelled">
          coming in {meta.arrives}
        </span>
      );
  }
}

/** Text for the (i) tooltip and the screen-reader description: help plus the real default. */
export function helpText(meta: FieldMeta): string {
  if (meta.kind === 'game-setting') return `${meta.help} ${meta.standsFor}`;
  return `${meta.help} Kafka default: ${describeDefault(meta.kafkaDefault, meta.control)}.`;
}

/** The visible line under the control, if the field has one. */
export function helperLine(meta: FieldMeta): string | undefined {
  switch (meta.kind) {
    case 'game-tuned':
      // Q5: a shortened value always shows the real default beside it (CLAUDE.md rule 6).
      return `Kafka default: ${describeDefault(meta.kafkaDefault, meta.control)}`;
    case 'not-modelled':
      return `Not simulated yet. Kafka default: ${describeDefault(meta.kafkaDefault, meta.control)}.`;
    case 'kafka':
    case 'game-setting':
      return meta.readOnly;
  }
}

export interface FieldFrameProps {
  /** Id of the control the label points at. */
  readonly inputId: string;
  /** Data attribute for tests and styling, e.g. "fetchMaxWaitMs". */
  readonly field: string;
  readonly label: string;
  readonly kafkaName?: string | undefined;
  readonly chip?: ReactNode;
  /** A chip at the start of the helper line (not-modelled fields: keeps the config name whole). */
  readonly helperChip?: ReactNode;
  /** (i) tooltip text; also read out through aria-describedby. */
  readonly help: string;
  readonly helper?: string | undefined;
  readonly error?: FieldError | null | undefined;
  /** Renders the control with the ids it must put in `aria-describedby`. */
  readonly children: (describedBy: string) => ReactNode;
}

/**
 * One field row, 328 wide and at least 56 tall (ui-spec "Right panel contents"): the label with
 * the real Kafka config name (this is a learning tool), the kind chip and an (i) tooltip on the
 * first line, the control below, then an optional helper line and the error. Helper and error
 * lines make the row taller rather than squeezing the control.
 */
export function FieldFrame(props: FieldFrameProps) {
  const { inputId, field, label, kafkaName, chip, helperChip, help, helper, error, children } =
    props;
  const helpId = `${inputId}-help`;
  const helperId = `${inputId}-helper`;
  const errorId = `${inputId}-error`;
  const describedBy = [
    helpId,
    helper === undefined ? null : helperId,
    error == null ? null : errorId,
  ]
    .filter((id) => id !== null)
    .join(' ');
  return (
    <div className="flex min-h-14 w-[328px] flex-col gap-1" data-field={field}>
      <div className="flex h-[20px] items-center gap-1.5 text-xs">
        <label htmlFor={inputId} className="shrink-0 font-medium">
          {label}
        </label>
        {kafkaName !== undefined && (
          <code className="min-w-0 truncate font-mono text-[11px] text-status/70" title={kafkaName}>
            {kafkaName}
          </code>
        )}
        <span className="ml-auto flex shrink-0 items-center gap-1.5">
          {chip}
          <Tip text={help}>
            <span
              tabIndex={0}
              role="img"
              aria-label={`About ${label}`}
              className={`inline-flex size-4 cursor-help items-center justify-center rounded-full border border-border font-serif text-[10px] italic text-status/70 ${FOCUS_RING}`}
            >
              i
            </span>
          </Tip>
        </span>
      </div>
      {children(describedBy)}
      <span id={helpId} className="sr-only">
        {help}
      </span>
      {helper !== undefined && (
        <p id={helperId} className="text-[11px] leading-snug text-status/70">
          {helperChip !== undefined && <span className="mr-1.5">{helperChip}</span>}
          {helper}
        </p>
      )}
      {error != null && (
        <p
          id={errorId}
          className="text-[11px] leading-snug text-danger-text"
          data-error-source={error.source}
        >
          {errorText(error)}
        </p>
      )}
    </div>
  );
}
