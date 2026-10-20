/**
 * Pure helpers for the properties panel (M2-T10): turning what the user typed into a config value,
 * validating it with the field's Zod schema, and printing values and Kafka defaults with units.
 * No React, no DOM.
 */

import type { ComponentKind } from '@kp/schema';
import { fieldSchema, type Control, type KafkaDefault, type Unit } from './field-meta.ts';

export type FieldCheck =
  { readonly ok: true; readonly value: unknown } | { readonly ok: false; readonly error: string };

const MS_UNITS: readonly (readonly [number, string, string])[] = [
  [86_400_000, 'day', 'days'],
  [3_600_000, 'h', 'h'],
  [60_000, 'min', 'min'],
  [1000, 's', 's'],
];

/** 300000 → "5 min", 45000 → "45 s", 604800000 → "7 days", 1500 → "1.5 s", 500 → "500 ms". */
export function humanDuration(ms: number): string {
  for (const [size, one, many] of MS_UNITS) {
    if (ms >= size && ms % size === 0) {
      const n = ms / size;
      return `${String(n)} ${n === 1 ? one : many}`;
    }
  }
  if (ms >= 1000) return `${String(ms / 1000)} s`;
  return `${String(ms)} ms`;
}

/** A value with its unit, plus a friendlier form where it helps ("300000 ms (5 min)"). */
export function withUnit(value: number, unit: Unit | undefined): string {
  switch (unit) {
    case undefined:
      return String(value);
    case 'ms':
      return value >= 1000
        ? `${String(value)} ms (${humanDuration(value)})`
        : `${String(value)} ms`;
    case 'bytes':
      return value >= 1024 && value % 1024 === 0
        ? `${String(value)} bytes (${String(value / 1024)} KiB)`
        : `${String(value)} ${value === 1 ? 'byte' : 'bytes'}`;
    case 'msg/s':
      return `${String(value)} msg/s`;
    case 'records':
    case 'replicas':
      // "1 replica", "500 records".
      return `${String(value)} ${value === 1 ? unit.slice(0, -1) : unit}`;
  }
}

/** Prints a config value for helper lines and tooltips. */
export function describeValue(value: unknown, control: Control): string {
  if (value === null || value === undefined) return 'none';
  if (typeof value === 'number') {
    return control.type === 'int' || control.type === 'number'
      ? withUnit(value, control.unit)
      : String(value);
  }
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

export function describeDefault(kafkaDefault: KafkaDefault, control: Control): string {
  return 'none' in kafkaDefault ? kafkaDefault.none : describeValue(kafkaDefault.value, control);
}

/** What an input box shows for a value: plain numbers (no separators), "" for null. */
export function toInputText(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return JSON.stringify(value);
}

/**
 * Typed text → value for a text-box control. Numbers go through `Number` so "1.5" or "abc" reach
 * Zod and get its message ("… must be a whole number."); only an empty box is caught here.
 */
export function fromInputText(text: string, control: Control): FieldCheck {
  if (control.type === 'int' || control.type === 'number' || control.type === 'partitions') {
    const trimmed = text.trim();
    if (trimmed === '') return { ok: false, error: 'Enter a number.' };
    const n = Number(trimmed);
    return { ok: true, value: Number.isNaN(n) ? trimmed : n };
  }
  return { ok: true, value: text };
}

/** Validates one field's value with its schema from `@kp/schema` (inline validation). */
export function checkField(kind: ComponentKind, field: string, value: unknown): FieldCheck {
  const schema = fieldSchema(kind, field);
  if (schema === undefined) return { ok: false, error: `Unknown field "${field}".` };
  const result = schema.safeParse(value);
  if (result.success) return { ok: true, value: result.data };
  return { ok: false, error: result.error.issues.map((i) => i.message).join(' ') };
}

/** Text → validated value in one step. */
export function parseFieldText(
  kind: ComponentKind,
  field: string,
  text: string,
  control: Control,
): FieldCheck {
  const raw = fromInputText(text, control);
  return raw.ok ? checkField(kind, field, raw.value) : raw;
}

/** "D, E ,F,," → ["D", "E", "F"]: the key lists of the random and skewed strategies. */
export function splitKeys(text: string): string[] {
  return text
    .split(',')
    .map((k) => k.trim())
    .filter((k) => k !== '');
}
