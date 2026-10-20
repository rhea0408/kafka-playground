import { describe, expect, it } from 'vitest';
import { createRng } from '../core/rng.ts';
import { placePartitions } from './placement.ts';

describe('placePartitions', () => {
  it('puts every partition on the only broker', () => {
    expect(placePartitions(3, ['b1'], createRng(1))).toEqual(['b1', 'b1', 'b1']);
  });

  it('spreads partitions round-robin over brokers sorted by id', () => {
    const leaders = placePartitions(6, ['b3', 'b1', 'b2'], createRng(1));
    const start = ['b1', 'b2', 'b3'].indexOf(leaders[0] ?? '');
    expect(leaders).toEqual(
      Array.from({ length: 6 }, (_, p) => ['b1', 'b2', 'b3'][(start + p) % 3]),
    );
  });

  it('is deterministic for the same RNG stream', () => {
    expect(placePartitions(4, ['a', 'b'], createRng(9).fork('placement:t'))).toEqual(
      placePartitions(4, ['a', 'b'], createRng(9).fork('placement:t')),
    );
  });

  it('needs a broker', () => {
    expect(() => placePartitions(1, [], createRng(1))).toThrow(/at least one broker/);
  });
});
