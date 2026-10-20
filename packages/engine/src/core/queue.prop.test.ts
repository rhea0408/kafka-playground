import fc from 'fast-check';
import { describe, it } from 'vitest';
import { createEventQueue } from './queue';

type Op = { kind: 'push'; time: number } | { kind: 'pop' };

const opArb: fc.Arbitrary<Op> = fc.oneof(
  // A small time range forces many ties, which is what the seq tie-break is for.
  fc.record({ kind: fc.constant('push' as const), time: fc.integer({ min: 0, max: 20 }) }),
  fc.record({ kind: fc.constant('pop' as const) }),
);

describe('event queue properties', () => {
  it('always pops the minimum (time, seq) among pending entries, matching a sorted reference', () => {
    fc.assert(
      fc.property(fc.array(opArb, { maxLength: 300 }), (ops) => {
        const q = createEventQueue<number>();
        // Reference model: a plain array kept sorted by (time, insertion index).
        const pending: { time: number; id: number }[] = [];
        let id = 0;
        for (const op of ops) {
          if (op.kind === 'push') {
            q.push(op.time, id);
            pending.push({ time: op.time, id });
            id++;
          } else {
            pending.sort((a, b) => a.time - b.time || a.id - b.id);
            const expected = pending.shift();
            const actual = q.pop();
            if (expected?.id !== actual?.item) return false;
          }
          if (q.size !== pending.length) return false;
        }
        return true;
      }),
    );
  });
});
