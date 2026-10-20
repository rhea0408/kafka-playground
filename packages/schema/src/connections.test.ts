import { describe, expect, it } from 'vitest';
import { COMPONENT_KINDS } from './components.ts';
import { canConnect } from './connections.ts';

describe('canConnect', () => {
  it('allows only producer → topic and topic → consumer', () => {
    const allowed: string[] = [];
    for (const from of COMPONENT_KINDS) {
      for (const to of COMPONENT_KINDS) {
        if (canConnect(from, to).ok) allowed.push(`${from}→${to}`);
      }
    }
    expect(allowed).toEqual(['topic→consumer', 'producer→topic']);
  });

  it('explains every rejected pair in a full sentence', () => {
    for (const from of COMPONENT_KINDS) {
      for (const to of COMPONENT_KINDS) {
        const check = canConnect(from, to);
        if (!check.ok) expect(check.reason).toMatch(/^[A-Z].{20,}\.$/);
      }
    }
  });

  it('gives a specific reason for the common mistakes', () => {
    expect(canConnect('producer', 'consumer')).toEqual({
      ok: false,
      reason:
        "A producer can't send straight to a consumer. Producers write to a topic, and consumers read from it.",
    });
    expect(canConnect('consumer', 'topic')).toMatchObject({
      ok: false,
      reason: /topic → consumer/,
    });
    expect(canConnect('producer', 'broker')).toMatchObject({ ok: false, reason: /automatically/ });
  });
});
