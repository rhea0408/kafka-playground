import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { ActionsProvider } from './actions/actions-context.tsx';
import { App } from './App.tsx';
import { startSim } from './sim/bootstrap.ts';
import './index.css';

const root = document.getElementById('root');
if (root === null) throw new Error('Missing #root element in index.html');

// One sim worker for the page's lifetime, started outside React so StrictMode's double effects
// don't create two.
const sim = startSim();

createRoot(root).render(
  <StrictMode>
    <ActionsProvider actions={sim.actions}>
      <App />
    </ActionsProvider>
  </StrictMode>,
);
