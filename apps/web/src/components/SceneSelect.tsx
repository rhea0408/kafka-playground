import { useState } from 'react';
import { useActions } from '../actions/actions-context.tsx';
import { sceneById, SCENES, type SceneId } from '../stores/default-scene.ts';
import { resetTopologyStore } from '../stores/topology-store.ts';
import { select, EMPTY_SELECTION } from '../stores/ui-store.ts';

/**
 * Scene presets (M3-T12): loads a ready-made canvas and starts the sim again from t = 0, paused,
 * like Reset does with the current canvas. The hint says what to try.
 */
export function SceneSelect({ initial }: { initial: SceneId }) {
  const actions = useActions();
  const [scene, setScene] = useState<SceneId>(initial);
  const hint = sceneById(scene)?.hint ?? '';
  return (
    <div className="flex items-center gap-1.5 text-[12px]">
      <label htmlFor="scene-select" className="text-status/70">
        Scene
      </label>
      <select
        id="scene-select"
        className="h-8 rounded-sm border border-border bg-surface px-1 text-[12px] focus-visible:outline-2 focus-visible:outline-primary"
        value={scene}
        title={hint}
        aria-describedby="scene-hint"
        onChange={(event) => {
          const next = sceneById(event.target.value);
          if (next === undefined) return;
          setScene(next.id);
          select(EMPTY_SELECTION);
          resetTopologyStore(next.state());
          void actions.pause().then(() => actions.reset());
        }}
      >
        {SCENES.map((s) => (
          <option key={s.id} value={s.id}>
            {s.label}
          </option>
        ))}
      </select>
      <span id="scene-hint" className="sr-only">
        {hint}
      </span>
    </div>
  );
}
