import { useEffect, useId, useRef, useState } from 'react';
import {
  ALL_EVENT_TYPES,
  EVENT_TYPE_GROUPS,
  filterSummary,
  NO_HIDDEN_TYPES,
  setGroup,
  toggleType,
  type HiddenTypes,
} from './event-filter.ts';

const SMALL_BUTTON =
  'inline-flex h-6 items-center rounded-sm border border-border bg-surface px-2 text-[12px] text-status hover:bg-muted ' +
  'focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-primary';

/**
 * Event-type filter (B7): a disclosure button that opens a panel of labelled checkboxes, one per
 * engine event type, in groups. Escape or the button closes it; focus goes back to the button.
 */
export function EventFilter({
  hidden,
  onChange,
}: {
  hidden: HiddenTypes;
  onChange: (hidden: HiddenTypes) => void;
}) {
  const [open, setOpen] = useState(false);
  const panelId = useId();
  const button = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open) panel.current?.querySelector('input')?.focus();
  }, [open]);

  const close = (): void => {
    setOpen(false);
    button.current?.focus();
  };

  return (
    <div className="relative">
      <button
        ref={button}
        type="button"
        aria-expanded={open}
        aria-controls={panelId}
        className={SMALL_BUTTON}
        onClick={() => {
          setOpen(!open);
        }}
      >
        Filter: {filterSummary(hidden)}
      </button>
      {open && (
        <div
          ref={panel}
          id={panelId}
          role="group"
          aria-label="Event types to show"
          className="absolute right-0 bottom-7 z-50 flex w-[600px] flex-col gap-2 rounded-md border border-border bg-surface p-3 text-[12px] shadow-md"
          onKeyDown={(event) => {
            if (event.key === 'Escape') {
              event.stopPropagation();
              close();
            }
          }}
        >
          <div className="grid grid-cols-4 gap-x-3">
            {EVENT_TYPE_GROUPS.map((group) => {
              const allShown = group.types.every((t) => !hidden.has(t));
              return (
                <fieldset key={group.label} className="flex flex-col gap-0.5">
                  <legend className="mb-0.5 flex w-full items-center gap-2 text-[11px] font-bold text-status/70">
                    {group.label.toUpperCase()}
                    <button
                      type="button"
                      aria-label={`${allShown ? 'Hide' : 'Show'} ${group.label.toLowerCase()}`}
                      className="rounded-sm px-1 text-[11px] font-normal text-primary underline focus-visible:outline-2 focus-visible:outline-primary"
                      onClick={() => {
                        onChange(setGroup(hidden, group.types, !allShown));
                      }}
                    >
                      {allShown ? 'hide' : 'show'}
                    </button>
                  </legend>
                  {group.types.map((type) => (
                    <label key={type} className="flex h-5 items-center gap-2 font-mono">
                      <input
                        type="checkbox"
                        className="size-3.5 accent-primary focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-primary"
                        checked={!hidden.has(type)}
                        onChange={() => {
                          onChange(toggleType(hidden, type));
                        }}
                      />
                      {type}
                    </label>
                  ))}
                </fieldset>
              );
            })}
          </div>
          <div className="flex gap-2 border-t border-divider pt-2">
            <button
              type="button"
              className={SMALL_BUTTON}
              onClick={() => {
                onChange(NO_HIDDEN_TYPES);
              }}
            >
              Show all
            </button>
            <button
              type="button"
              className={SMALL_BUTTON}
              onClick={() => {
                onChange(new Set(ALL_EVENT_TYPES));
              }}
            >
              Hide all
            </button>
            <span className="flex-1" />
            <button type="button" className={SMALL_BUTTON} onClick={close}>
              Done
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
