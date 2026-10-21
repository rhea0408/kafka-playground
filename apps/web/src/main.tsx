import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { ActionsProvider } from './actions/actions-context.tsx';
import { App } from './App.tsx';
import { startSim } from './sim/bootstrap.ts';
import { installSimDebugHook } from './sim/debug-hook.ts';
import { initialSceneFor } from './stores/default-scene.ts';
import { resetTopologyStore } from './stores/topology-store.ts';
import './index.css';

const root = document.getElementById('root');
if (root === null) throw new Error('Missing #root element in index.html');

// The Sandbox opens with the default scene, paused (`?scene=empty`: broker only). M2-T12.
resetTopologyStore(initialSceneFor(window.location.search));

// One sim worker for the page's lifetime, started outside React so StrictMode's double effects
// don't create two.
const sim = startSim();
installSimDebugHook(sim.debugHook);

createRoot(root).render(
  <StrictMode>
    <ActionsProvider actions={sim.actions}>
      <App />
    </ActionsProvider>
  </StrictMode>,
);
