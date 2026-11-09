import { describe, expect, it } from 'vitest';
import { isrState } from './isr-state.ts';

const replica = (broker: string) => ({ broker, logEndOffset: 0, highWatermark: 0 });
const rf3 = {
  replicas: ['b1', 'b2', 'b3'].map(replica),
  leader: 'b1' as string | null,
  underReplicated: false,
  underMinIsr: false,
};

describe('isrState (M4 follow-up G-3)', () => {
  it('is in sync when every replica is', () => {
    expect(isrState(rf3)).toBe('in-sync');
  });

  it('under min ISR wins over under-replicated', () => {
    expect(isrState({ ...rf3, underReplicated: true })).toBe('under-replicated');
    expect(isrState({ ...rf3, underReplicated: true, underMinIsr: true })).toBe('under-min-isr');
  });

  it('is never degraded for RF 1 or an offline partition', () => {
    const flags = { underReplicated: true, underMinIsr: true };
    expect(isrState({ ...rf3, ...flags, replicas: [replica('b1')] })).toBe('in-sync');
    expect(isrState({ ...rf3, ...flags, leader: null })).toBe('in-sync');
  });
});
