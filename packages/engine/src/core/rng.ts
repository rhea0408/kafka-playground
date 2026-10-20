/**
 * Seeded pseudo-random number generator (sfc32, Chris Doty-Humphrey's "Small Fast Counting"
 * generator from PractRand). The engine must never call Math.random (NFR-4.1, CLAUDE.md rule 2):
 * every random choice comes from an Rng created from the run's seed.
 *
 * All arithmetic is 32-bit integer (Math.imul, |0, >>>0), so the sequence is bit-identical in
 * every JavaScript engine. next() divides a uint32 by 2^32, which is exact in IEEE-754 doubles.
 */

export interface Rng {
  /** Uniform uint32 in [0, 2^32). */
  readonly nextUint32: () => number;
  /** Uniform float in [0, 1). */
  readonly next: () => number;
  /** Uniform integer in [min, max], both inclusive. */
  readonly int: (min: number, max: number) => number;
  /** Uniform element of a non-empty array. */
  readonly pick: <T>(items: readonly T[]) => T;
  /**
   * Independent child generator for one consumer of randomness, e.g. `fork('producer:p1')`.
   * The child depends only on this generator's seed and the label, never on how many numbers
   * have been drawn so far, so adding a new component does not shift the streams of others.
   */
  readonly fork: (label: string) => Rng;
}

const UINT32_RANGE = 0x1_0000_0000;

/** Creates a generator from an integer seed in [0, 2^32). */
export function createRng(seed: number): Rng {
  if (!Number.isInteger(seed) || seed < 0 || seed >= UINT32_RANGE) {
    throw new RangeError(`seed must be an integer in [0, 2^32), got ${String(seed)}`);
  }
  return fromSeedWords(expandSeed(seed));
}

type SeedWords = readonly [number, number, number, number];

function fromSeedWords(seedWords: SeedWords): Rng {
  let [a, b, c, d] = seedWords;

  const nextUint32 = (): number => {
    const t = (((a + b) | 0) + d) | 0;
    d = (d + 1) | 0;
    a = b ^ (b >>> 9);
    b = (c + (c << 3)) | 0;
    c = (c << 21) | (c >>> 11);
    c = (c + t) | 0;
    return t >>> 0;
  };

  // Warm-up: mix the seed into all four words before the first output.
  for (let i = 0; i < 12; i++) nextUint32();

  const int = (min: number, max: number): number => {
    if (!Number.isInteger(min) || !Number.isInteger(max) || min > max) {
      throw new RangeError(
        `int(min, max) needs integers with min <= max, got ${String(min)}, ${String(max)}`,
      );
    }
    const span = max - min + 1;
    if (span > UINT32_RANGE) throw new RangeError('int(min, max) range must fit in 2^32 values');
    // Rejection sampling avoids the modulo bias of `nextUint32() % span`.
    const limit = UINT32_RANGE - (UINT32_RANGE % span);
    let x = nextUint32();
    while (x >= limit) x = nextUint32();
    return min + (x % span);
  };

  return {
    nextUint32,
    next: () => nextUint32() / UINT32_RANGE,
    int,
    pick: <T>(items: readonly T[]): T => {
      if (items.length === 0) throw new RangeError('pick() needs a non-empty array');
      return items[int(0, items.length - 1)] as T;
    },
    fork: (label: string) => {
      const h = fnv1a32(label);
      return fromSeedWords([
        seedWords[0] ^ h,
        seedWords[1] ^ Math.imul(h, 0x85ebca6b),
        seedWords[2] ^ Math.imul(h, 0xc2b2ae35),
        seedWords[3] ^ Math.imul(h ^ (h >>> 16), 0x27d4eb2f),
      ]);
    },
  };
}

/** splitmix32: spreads one 32-bit seed over sfc32's four state words. */
function expandSeed(seed: number): SeedWords {
  let state = seed | 0;
  const next = (): number => {
    state = (state + 0x9e3779b9) | 0;
    let z = state;
    z = Math.imul(z ^ (z >>> 16), 0x85ebca6b);
    z = Math.imul(z ^ (z >>> 13), 0xc2b2ae35);
    return (z ^ (z >>> 16)) | 0;
  };
  return [next(), next(), next(), next()];
}

/** 32-bit FNV-1a over the label's UTF-16 code units. */
function fnv1a32(label: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < label.length; i++) {
    h = Math.imul(h ^ label.charCodeAt(i), 0x01000193);
  }
  return h | 0;
}
