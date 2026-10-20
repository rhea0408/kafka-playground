import { SimClock, SimControls } from '../SimControls.tsx';

/**
 * Top bar, full width × 56 (ui-spec, wireframe 05). The wireframe's Home link, Save / Load /
 * Export, Tips and "Live: real Kafka" belong to later milestones (see M2-decisions T6).
 */
export function TopBar() {
  return (
    <header className="relative flex items-center gap-4 border-b-[1.5px] border-border bg-surface px-4 [grid-area:top]">
      <h1 className="text-[15px] font-semibold text-status">
        Kafka Playground <span className="font-normal text-status/70">·</span> Sandbox
      </h1>
      <SimClock />
      <SimControls />
    </header>
  );
}
