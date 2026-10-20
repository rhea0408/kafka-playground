import { formatSimTime } from '@kp/engine';

/** M2-T0 placeholder: the themed top bar. The Sandbox layout (screen 05) arrives in T6. */
export function App() {
  return (
    <div className="flex h-full flex-col bg-surface">
      <header className="relative flex h-14 shrink-0 items-center border-b border-border px-4">
        <h1 className="text-base font-semibold text-status">Kafka Playground</h1>
        <span className="ml-3 rounded bg-muted px-2 py-0.5 font-mono text-xs text-status">
          Sandbox
        </span>
        <time className="absolute right-2 font-mono text-sm text-status" aria-label="Sim time">
          {formatSimTime(0)}
        </time>
      </header>
      <main className="flex-1" />
    </div>
  );
}
