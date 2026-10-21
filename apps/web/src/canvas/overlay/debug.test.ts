import { describe, expect, it } from 'vitest';
import { INITIAL_TOPOLOGY_STATE, type TopologyState } from '../../stores/topology-store.ts';
import { debugEnabled, newOverlayDebug, testDotSpecs } from './debug.ts';

describe('debugEnabled', () => {
  it('is on in dev builds or with ?fps', () => {
    expect(debugEnabled('', true)).toBe(true);
    expect(debugEnabled('', false)).toBe(false);
    expect(debugEnabled('?fps', false)).toBe(true);
    expect(debugEnabled('?a=1&fps=1', false)).toBe(true);
  });
});

describe('testDotSpecs', () => {
  const state: TopologyState = {
    ...INITIAL_TOPOLOGY_STATE,
    components: [
      ...INITIAL_TOPOLOGY_STATE.components,
      {
        kind: 'producer',
        id: 'p1',
        ratePerSec: 2,
        keyStrategy: { kind: 'null' },
        acks: 'all',
        batchSize: 16384,
        messageSizeBytes: 100,
      } as unknown as TopologyState['components'][number],
      {
        kind: 'topic',
        id: 't1',
        partitions: 3,
        replicationFactor: 1,
      } as unknown as TopologyState['components'][number],
    ],
    connections: [{ from: 'p1', to: 't1' }],
  };

  it('spreads A1 dots over the partitions and along the path, with unique trace ids', () => {
    const specs = testDotSpecs(state, 4, 2);
    expect(specs.map((s) => s.spec.to.handleId)).toEqual(['p0:in', 'p1:in', 'p2:in', 'p0:in']);
    expect(specs.map((s) => s.progress)).toEqual([0, 0.25, 0.5, 0.75]);
    expect(new Set(specs.map((s) => s.spec.traceId)).size).toBe(4);
    expect(specs[0]?.spec.traceId).toBe('stress2#0');
  });

  it('is empty without a producer → topic connection', () => {
    expect(testDotSpecs(INITIAL_TOPOLOGY_STATE, 10)).toEqual([]);
  });

  it('counters start at zero', () => {
    expect(newOverlayDebug()).toMatchObject({ dotsDrawn: 0, fps: 0, capChip: false });
  });
});
