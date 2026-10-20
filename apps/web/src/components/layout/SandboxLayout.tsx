import { ReactFlowProvider } from '@xyflow/react';
import { Canvas } from '../../canvas/Canvas.tsx';
import { Toasts } from '../Toasts.tsx';
import { BottomPanel } from './BottomPanel.tsx';
import { Palette } from './Palette.tsx';
import { RightPanel } from './RightPanel.tsx';
import { TopBar } from './TopBar.tsx';

/**
 * Sandbox screen (wireframe 05, ui-spec "Level screen layout"): top bar 56; palette 240 and right
 * panel 360, both full height below the top bar; the canvas flexes; the bottom panel is canvas
 * width × 200. Fills the viewport without scrolling; reference 1440 × 900, minimum 1366 × 768.
 * The React Flow provider wraps the whole screen so the palette can add nodes at the canvas centre.
 */
export function SandboxLayout() {
  return (
    <ReactFlowProvider>
      <div className="grid h-full grid-cols-[240px_minmax(0,1fr)_360px] grid-rows-[56px_minmax(0,1fr)_200px] overflow-hidden bg-surface [grid-template-areas:'top_top_top'_'palette_canvas_right'_'palette_bottom_right']">
        <TopBar />
        <Palette />
        <main
          aria-label="Canvas"
          className="relative min-h-0 min-w-0 bg-surface [grid-area:canvas]"
        >
          {/* The message overlay (Canvas2D) arrives in T9. */}
          <Canvas />
          <Toasts />
        </main>
        <BottomPanel />
        <RightPanel />
      </div>
    </ReactFlowProvider>
  );
}
