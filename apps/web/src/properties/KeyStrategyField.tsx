import type { KeyStrategy } from '@kp/schema';
import { useState, type KeyboardEvent } from 'react';
import { INPUT } from './controls.tsx';
import type { FieldError } from './FieldFrame.tsx';
import { checkField, splitKeys } from './field-value.ts';

const STRATEGY_LABELS: Readonly<Record<KeyStrategy['type'], string>> = {
  null: 'No key (null)',
  fixed: 'One fixed key',
  random: 'Random from a list',
  skewed: 'Skewed: one hot key',
};

const STRATEGY_TYPES = Object.keys(STRATEGY_LABELS) as KeyStrategy['type'][];

/**
 * The strategy a type switch starts from. D, E and F hash to partitions 0, 1 and 2 of a
 * 3-partition topic (murmur2; A, B and C all land on partition 1, see M2-web-shell T12), so the
 * starting keys actually spread.
 */
export function defaultStrategy(type: KeyStrategy['type']): KeyStrategy {
  switch (type) {
    case 'null':
      return { type: 'null' };
    case 'fixed':
      return { type: 'fixed', key: 'D' };
    case 'random':
      return { type: 'random', keys: ['D', 'E', 'F'] };
    case 'skewed':
      return { type: 'skewed', hotKey: 'D', hotShare: 0.8, otherKeys: ['E', 'F'] };
  }
}

type Part = 'key' | 'keys' | 'hotKey' | 'hotShare' | 'otherKeys';

/** The text a part's box shows for the committed strategy. */
function partText(strategy: KeyStrategy, part: Part): string {
  switch (strategy.type) {
    case 'null':
      return '';
    case 'fixed':
      return part === 'key' ? strategy.key : '';
    case 'random':
      return part === 'keys' ? strategy.keys.join(', ') : '';
    case 'skewed':
      if (part === 'hotKey') return strategy.hotKey;
      if (part === 'hotShare') return String(strategy.hotShare);
      if (part === 'otherKeys') return strategy.otherKeys.join(', ');
      return '';
  }
}

/** The committed strategy with one part replaced by typed text (not validated yet). */
export function withPart(strategy: KeyStrategy, part: Part, text: string): unknown {
  switch (part) {
    case 'key':
      return { ...strategy, key: text };
    case 'keys':
      return { ...strategy, keys: splitKeys(text) };
    case 'hotKey':
      return { ...strategy, hotKey: text };
    case 'hotShare': {
      const n = Number(text.trim());
      return { ...strategy, hotShare: text.trim() === '' || Number.isNaN(n) ? text : n };
    }
    case 'otherKeys':
      return { ...strategy, otherKeys: splitKeys(text) };
  }
}

const PARTS: Readonly<Record<KeyStrategy['type'], readonly { part: Part; label: string }[]>> = {
  null: [],
  fixed: [{ part: 'key', label: 'Key' }],
  random: [{ part: 'keys', label: 'Keys (comma-separated)' }],
  skewed: [
    { part: 'hotKey', label: 'Hot key' },
    { part: 'hotShare', label: 'Hot-key share (0 to 1)' },
    { part: 'otherKeys', label: 'Other keys (comma-separated)' },
  ],
};

export interface KeyStrategyFieldProps {
  readonly inputId: string;
  readonly value: KeyStrategy;
  readonly describedBy: string;
  readonly error: FieldError | null;
  readonly setError: (error: FieldError | null) => void;
  readonly commit: (value: unknown) => Promise<string | null>;
}

/**
 * Producer keys (a game setting: real code sets the key on each ProducerRecord). A picker for the
 * strategy, then its keys. Switching strategy saves at once; key boxes save on blur or Enter, each
 * validated with `KeyStrategySchema` as a whole.
 */
export function KeyStrategyField(props: KeyStrategyFieldProps) {
  const { inputId, value, describedBy, error, setError, commit } = props;
  const [drafts, setDrafts] = useState<Partial<Record<Part, string>>>({});

  const send = async (next: unknown): Promise<void> => {
    const checked = checkField('producer', 'keyStrategy', next);
    if (!checked.ok) {
      setError({ source: 'schema', message: checked.error });
      return;
    }
    const reason = await commit(checked.value);
    if (reason === null) {
      setDrafts({});
      setError(null);
    } else {
      setError({ source: 'engine', message: reason });
    }
  };

  const savePart = (part: Part): void => {
    const draft = drafts[part];
    if (draft === undefined || draft === partText(value, part)) return;
    void send(withPart(value, part, draft));
  };

  const onKeyDown = (part: Part) => (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      savePart(part);
    } else if (event.key === 'Escape') {
      setDrafts({});
      setError(null);
    }
  };

  return (
    <div className="flex flex-col gap-1.5">
      <select
        id={inputId}
        className={INPUT}
        value={value.type}
        aria-describedby={describedBy}
        aria-invalid={error !== null}
        onChange={(event) => {
          const type = STRATEGY_TYPES.find((t) => t === event.target.value);
          if (type === undefined) return;
          setDrafts({});
          void send(defaultStrategy(type));
        }}
      >
        {STRATEGY_TYPES.map((t) => (
          <option key={t} value={t}>
            {STRATEGY_LABELS[t]}
          </option>
        ))}
      </select>
      {PARTS[value.type].map(({ part, label }) => {
        const id = `${inputId}-${part}`;
        return (
          <div key={part} className="flex items-center gap-2">
            <label htmlFor={id} className="w-[132px] shrink-0 text-[11px] text-status/70">
              {label}
            </label>
            <input
              id={id}
              type="text"
              inputMode={part === 'hotShare' ? 'decimal' : 'text'}
              className={INPUT}
              value={drafts[part] ?? partText(value, part)}
              aria-describedby={describedBy}
              aria-invalid={error !== null}
              onChange={(event) => {
                const text = event.target.value;
                setDrafts({ ...drafts, [part]: text });
                const checked = checkField('producer', 'keyStrategy', withPart(value, part, text));
                setError(checked.ok ? null : { source: 'schema', message: checked.error });
              }}
              onBlur={() => {
                savePart(part);
              }}
              onKeyDown={onKeyDown(part)}
            />
          </div>
        );
      })}
    </div>
  );
}
