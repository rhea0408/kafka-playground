import { describe, expect, it, vi, beforeEach } from 'vitest';
import type { Actions } from '../actions/actions.ts';
import { resetTopologyStore, INITIAL_TOPOLOGY_STATE } from '../stores/topology-store.ts';
import { resetUiStore, useUiStore } from '../stores/ui-store.ts';
import { connectChecked, deleteSelection, isComponentKind } from './canvas-commands.ts';

const OK = { ok: true } as const;

describe('connectChecked', () => {
  beforeEach(() => {
    resetUiStore();
    resetTopologyStore({
      ...INITIAL_TOPOLOGY_STATE,
      components: [
        ...INITIAL_TOPOLOGY_STATE.components,
        {
          kind: 'producer',
          id: 'p1',
          ratePerSec: 1,
          keyStrategy: { type: 'null' },
          acks: 'all',
          batchSize: 16384,
          messageSizeBytes: 1024,
          metadataMaxAgeMs: 300_000,
        },
        { kind: 'topic', id: 't1', partitions: 1, replicationFactor: 1, minInsyncReplicas: 1 },
      ],
    });
  });

  it('sends a valid connection to the actions', async () => {
    const connect = vi.fn<Actions['connect']>(() => Promise.resolve(OK));
    expect(await connectChecked({ connect }, 'p1', 't1')).toEqual(OK);
    expect(connect).toHaveBeenCalledWith('p1', 't1');
    expect(useUiStore.getState().toasts).toEqual([]);
  });

  it('shows the reason and sends nothing for an invalid one', async () => {
    const connect = vi.fn<Actions['connect']>(() => Promise.resolve(OK));
    const result = await connectChecked({ connect }, 't1', 'p1');
    expect(result.ok).toBe(false);
    expect(connect).not.toHaveBeenCalled();
    expect(useUiStore.getState().toasts.map((t) => [t.kind, t.message])).toEqual([
      ['warning', 'Data flows from producers into topics. Connect producer → topic instead.'],
    ]);
  });
});

describe('deleteSelection', () => {
  it('disconnects selected edges first, then removes nodes, skipping edges of removed nodes', async () => {
    const calls: string[] = [];
    const actions = {
      disconnect: (from: string, to: string) => {
        calls.push(`disconnect ${from} ${to}`);
        return Promise.resolve(OK);
      },
      removeComponent: (id: string) => {
        calls.push(`remove ${id}`);
        return Promise.resolve(OK);
      },
    };
    await deleteSelection(actions, ['c1', 'p2'], ['p1→t1', 't1→c1', 'garbage']);
    expect(calls).toEqual(['disconnect p1 t1', 'remove c1', 'remove p2']);
  });
});

describe('isComponentKind', () => {
  it('accepts only the four kinds', () => {
    expect(['producer', 'topic', 'broker', 'consumer'].every(isComponentKind)).toBe(true);
    expect(isComponentKind('')).toBe(false);
    expect(isComponentKind('cluster')).toBe(false);
  });
});
