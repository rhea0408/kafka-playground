import { COMPONENT_KINDS, parseComponent, type Component, type ComponentKind } from '@kp/schema';
import { describe, expect, it } from 'vitest';
import {
  EXTRA_FIELDS,
  FIELD_META,
  fieldSchema,
  fieldsOf,
  isEditable,
  metaOf,
  SHAPES,
  type FieldMeta,
} from './field-meta.ts';

/** A component with every schema default filled in. */
function defaults(kind: ComponentKind): Component {
  const parsed = parseComponent({ kind, id: 'x' });
  if (!parsed.ok) throw new Error(parsed.errors.join(' '));
  return parsed.value;
}

const value = (c: Component, field: string): unknown =>
  (c as unknown as Readonly<Record<string, unknown>>)[field];

const allMeta = (): { kind: ComponentKind; field: string; meta: FieldMeta }[] =>
  COMPONENT_KINDS.flatMap((kind) =>
    fieldsOf(kind).map((field) => ({ kind, field: String(field), meta: metaOf(kind, field) })),
  );

describe('field metadata', () => {
  it.each(COMPONENT_KINDS)('covers exactly the %s schema fields (minus kind and id)', (kind) => {
    const schemaFields = Object.keys(SHAPES[kind]).filter((k) => k !== 'kind' && k !== 'id');
    expect(Object.keys(FIELD_META[kind]).sort()).toEqual(schemaFields.sort());
  });

  it('every field has a Zod schema to validate it inline', () => {
    for (const { kind, field } of allMeta()) expect(fieldSchema(kind, field)).toBeDefined();
  });

  it('a kafka field’s schema default is the real Kafka default (else it is game-tuned)', () => {
    for (const { kind, field, meta } of allMeta()) {
      if (meta.kind !== 'kafka' && meta.kind !== 'not-modelled') continue;
      if (!('value' in meta.kafkaDefault)) continue;
      expect({ kind, field, value: value(defaults(kind), field) }).toEqual({
        kind,
        field,
        value: meta.kafkaDefault.value,
      });
    }
  });

  it('a game-tuned field’s schema default differs from the Kafka default it shows', () => {
    for (const { kind, field, meta } of allMeta()) {
      if (meta.kind !== 'game-tuned' || !('value' in meta.kafkaDefault)) continue;
      expect(value(defaults(kind), field)).not.toEqual(meta.kafkaDefault.value);
    }
  });

  it('the Q5 game settings are labelled as such and say what they stand in for', () => {
    expect(FIELD_META.producer.ratePerSec.kind).toBe('game-setting');
    expect(FIELD_META.producer.messageSizeBytes.kind).toBe('game-setting');
    expect(FIELD_META.consumer.processingTimeMs.kind).toBe('game-setting');
    for (const { meta } of allMeta()) {
      if (meta.kind === 'game-setting') expect(meta.standsFor).toMatch(/^Stands in for /);
    }
  });

  it('every real config shows its Kafka config name', () => {
    for (const { meta } of allMeta()) {
      if (meta.kind !== 'game-setting') expect(meta.kafkaName).not.toBe('');
    }
  });

  it('immutable M2 fields are read-only with a reason; not-modelled ones are disabled', () => {
    const fixed = allMeta()
      .filter(({ meta }) => !isEditable(meta))
      .map(({ kind, field }) => `${kind}.${field}`);
    expect(fixed.sort()).toEqual(
      [
        'broker.rack',
        'consumer.autoOffsetReset',
        'consumer.groupId',
        'topic.minInsyncReplicas',
        'topic.replicationFactor',
      ].sort(),
    );
  });

  it('extra (not-modelled) fields are not schema fields and name their milestone', () => {
    for (const kind of COMPONENT_KINDS) {
      for (const extra of EXTRA_FIELDS[kind]) {
        expect(Object.keys(SHAPES[kind])).not.toContain(extra.key);
        expect(extra.kind).toBe('not-modelled');
        expect(['M3', 'M4', 'v0.4']).toContain(extra.arrives);
      }
    }
  });
});
