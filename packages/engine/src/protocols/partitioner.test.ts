import { describe, expect, it } from 'vitest';
import { createRng } from '../core/rng.ts';
import {
  createStickyState,
  murmur2,
  partitionForKey,
  recordStickyAppend,
  stickyPartition,
  toPositive,
  utf8Bytes,
} from './partitioner.ts';

describe('murmur2 (ported from Kafka Utils.murmur2)', () => {
  // Expected values copied from Kafka's UtilsTest#testMurmur2
  // (clients/src/test/java/org/apache/kafka/common/utils/UtilsTest.java).
  it.each([
    ['21', -973932308],
    ['foobar', -790332482],
    ['a-little-bit-long-string', -985981536],
    ['a-little-bit-longer-string', -1486304829],
    ['lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8', -58897971],
    ['abc', 479470107],
  ])('murmur2(%j) = %i, as in the Java client', (input, expected) => {
    expect(murmur2(utf8Bytes(input))).toBe(expected);
  });

  it('handles every tail length (0-3 leftover bytes) and the empty array', () => {
    for (const s of ['', 'a', 'ab', 'abc', 'abcd', 'abcde']) {
      const h = murmur2(utf8Bytes(s));
      expect(Number.isInteger(h)).toBe(true);
      expect(h).toBeGreaterThanOrEqual(-(2 ** 31));
      expect(h).toBeLessThan(2 ** 31);
    }
  });
});

describe('toPositive', () => {
  it('clears the sign bit instead of taking the absolute value', () => {
    expect(toPositive(5)).toBe(5);
    expect(toPositive(-1)).toBe(0x7fffffff);
    expect(toPositive(-973932308)).toBe(-973932308 & 0x7fffffff);
    expect(toPositive(-(2 ** 31))).toBe(0);
  });
});

describe('utf8Bytes (like Java String#getBytes(UTF_8))', () => {
  it.each([
    ['A', [0x41]],
    ['é', [0xc3, 0xa9]],
    ['€', [0xe2, 0x82, 0xac]],
    ['😀', [0xf0, 0x9f, 0x98, 0x80]],
    ['\ud800', [0x3f]],
    ['x\udc00y', [0x78, 0x3f, 0x79]],
  ])('%j → %j', (s, bytes) => {
    expect([...utf8Bytes(s)]).toEqual(bytes);
  });
});

describe('partitionForKey', () => {
  it('always sends the same key to the same partition', () => {
    for (const key of ['A', 'B', 'C', 'customer-42', '']) {
      const first = partitionForKey(key, 6);
      for (let i = 0; i < 10; i++) expect(partitionForKey(key, 6)).toBe(first);
    }
  });

  it('matches toPositive(murmur2(bytes)) % n from the Java client', () => {
    expect(partitionForKey('foobar', 6)).toBe((-790332482 & 0x7fffffff) % 6);
    expect(partitionForKey('21', 3)).toBe((-973932308 & 0x7fffffff) % 3);
  });

  it('spreads 10k random keys roughly evenly', () => {
    const rng = createRng(2024);
    const counts = Array.from({ length: 6 }, () => 0);
    for (let i = 0; i < 10_000; i++) {
      const p = partitionForKey(`key-${String(rng.nextUint32())}`, 6);
      counts[p] = (counts[p] ?? 0) + 1;
    }
    for (const c of counts) {
      expect(c).toBeGreaterThan(1500);
      expect(c).toBeLessThan(1850);
    }
  });

  it('moves many keys when the partition count changes (why adding partitions breaks key order)', () => {
    const keys = Array.from({ length: 1000 }, (_, i) => `order-${String(i)}`);
    const moved = keys.filter((k) => partitionForKey(k, 3) !== partitionForKey(k, 4)).length;
    expect(moved).toBeGreaterThan(500);
  });
});

describe('sticky partitioner (records without a key)', () => {
  it('stays on one partition until batch.size bytes were produced, then switches', () => {
    const rng = createRng(7);
    const state = createStickyState();
    const chosen: number[] = [];
    // batch.size 4096 with 1024-byte records: 4 records per sticky batch.
    for (let i = 0; i < 40; i++) {
      chosen.push(stickyPartition(state, 3, rng));
      recordStickyAppend(state, 1024, 4096, 3, rng);
    }
    for (let batch = 0; batch < 10; batch++) {
      const group = chosen.slice(batch * 4, batch * 4 + 4);
      expect(new Set(group).size).toBe(1);
    }
    expect(new Set(chosen).size).toBeGreaterThan(1);
  });

  it('is deterministic for the same seed', () => {
    const run = () => {
      const rng = createRng(11);
      const state = createStickyState();
      return Array.from({ length: 50 }, () => {
        const p = stickyPartition(state, 4, rng);
        recordStickyAppend(state, 1000, 3000, 4, rng);
        return p;
      });
    };
    expect(run()).toEqual(run());
  });

  it('re-picks when the current partition no longer exists', () => {
    const rng = createRng(3);
    const state = { partition: 5, producedBytes: 100 };
    expect(stickyPartition(state, 3, rng)).toBeLessThan(3);
    expect(state.producedBytes).toBe(0);
  });

  it('treats batch.size 0 as 1, like KafkaProducer (switch after every record)', () => {
    const rng = createRng(1);
    const state = createStickyState();
    stickyPartition(state, 2, rng);
    recordStickyAppend(state, 10, 0, 2, rng);
    expect(state.producedBytes).toBe(0);
  });
});
