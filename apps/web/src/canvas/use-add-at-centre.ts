import type { ComponentKind } from '@kp/schema';
import { useStoreApi } from '@xyflow/react';
import { useCallback } from 'react';
import type { AddResult } from '../actions/actions.ts';
import { useActions } from '../actions/actions-context.tsx';
import { useTopologyStore } from '../stores/topology-store.ts';
import { centredTopLeft, containerToFlow, freePosition, nodeSize } from './geometry.ts';

/**
 * Adds a component at the centre of the visible canvas: the keyboard / click alternative to
 * dragging from the palette (NFR-7.4). A spot already taken moves the new node down-right.
 */
export function useAddAtCentre(): (kind: ComponentKind) => Promise<AddResult> {
  const actions = useActions();
  const store = useStoreApi();
  return useCallback(
    (kind) => {
      const { transform, width, height } = store.getState();
      const centre = containerToFlow(
        { x: width / 2, y: height / 2 },
        { x: transform[0], y: transform[1], zoom: transform[2] },
      );
      const position = freePosition(
        Object.values(useTopologyStore.getState().positions),
        centredTopLeft(centre, nodeSize(kind)),
      );
      return actions.addComponent(kind, position);
    },
    [actions, store],
  );
}
