import { formatSimTime } from '@kp/engine';
import { Toasts } from './components/Toasts.tsx';
import { useSimStore } from './stores/sim-store.ts';

/** Minimal shell until the Sandbox layout (screen 05) arrives in T6. */
export function App() {
  const t = useSimStore((s) => s.t);
  return (
    <div className="flex h-full flex-col bg-surface">
      <header className="relative flex h-14 shrink-0 items-center border-b border-border px-4">
        <h1 className="text-base font-semibold text-status">Kafka Playground</h1>
        <span className="ml-3 rounded bg-muted px-2 py-0.5 font-mono text-xs text-status">
          Sandbox
        </span>
        <time className="absolute right-2 font-mono text-sm text-status" aria-label="Sim time">
          {formatSimTime(t)}
        </time>
      </header>
      <main className="flex-1" />
      <Toasts />
    </div>
  );
}
