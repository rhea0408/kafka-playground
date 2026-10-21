/**
 * Event-log fingerprint for determinism checks (NFR-4.1, CLAUDE.md rule 2): 32-bit FNV-1a over the
 * JSON text of the event array. The same seed and command log must give the same hash in Node,
 * in the browser worker (M2-T12) and in CI. Not a security hash; a collision is only a missed
 * mismatch in a test.
 */

import type { EngineEvent } from './index.ts';

const FNV_OFFSET = 0x811c9dc5;
const FNV_PRIME = 0x01000193;

export interface EventHasher {
  /** Feeds one event, in emission order. */
  readonly add: (event: EngineEvent) => void;
  /** Hash of every event added so far: equal to `hashEvents` over the same events. */
  readonly digest: () => string;
}

/**
 * Incremental form of `hashEvents`, for a host that cannot keep the whole log in memory. It hashes
 * exactly the text `JSON.stringify(events)` would produce: `[`, the events joined by `,`, `]`.
 */
export function createEventHasher(): EventHasher {
  let h = mix(FNV_OFFSET, '[');
  let count = 0;
  return {
    add: (event) => {
      if (count > 0) h = mix(h, ',');
      h = mix(h, JSON.stringify(event));
      count++;
    },
    digest: () => hex(mix(h, ']')),
  };
}

/** 32-bit FNV-1a over `JSON.stringify(events)`, as 8 hex digits. */
export function hashEvents(events: readonly EngineEvent[]): string {
  return hex(mix(FNV_OFFSET, JSON.stringify(events)));
}

function mix(hash: number, text: string): number {
  let h = hash;
  for (let i = 0; i < text.length; i++) h = Math.imul(h ^ text.charCodeAt(i), FNV_PRIME);
  return h;
}

const hex = (h: number): string => (h >>> 0).toString(16).padStart(8, '0');
