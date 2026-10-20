import type { ComponentKind } from '@kp/schema';
import { PALETTE_MIME } from '../../canvas/canvas-commands.ts';
import { useAddAtCentre } from '../../canvas/use-add-at-centre.ts';

/** The four component kinds M2 can build. */
const ITEMS: readonly { readonly kind: ComponentKind; readonly label: string }[] = [
  { kind: 'producer', label: 'Producer' },
  { kind: 'topic', label: 'Topic' },
  { kind: 'broker', label: 'Broker' },
  { kind: 'consumer', label: 'Consumer' },
];

/**
 * Left palette, 240 px wide and full height below the top bar (ui-spec, wireframe 05). Items are
 * 208 × 56 buttons: drag one onto the canvas to add it where it is dropped, or click / press Enter
 * to add it at the centre of the canvas (keyboard access).
 */
export function Palette() {
  const addAtCentre = useAddAtCentre();
  return (
    <aside
      aria-label="Palette"
      className="flex flex-col gap-2 overflow-y-auto border-r-[1.5px] border-border bg-surface p-4 [grid-area:palette]"
    >
      <h2 id="palette-components" className="mt-[18px] text-xs font-semibold text-status/70">
        COMPONENTS
      </h2>
      <p id="palette-hint" className="sr-only">
        Drag onto the canvas, or press Enter to add at the centre of the canvas.
      </p>
      <ul aria-labelledby="palette-components" className="flex flex-col gap-2">
        {ITEMS.map(({ kind, label }) => (
          <li key={kind} data-testid="palette-item" className="h-14 w-[208px]">
            <button
              type="button"
              draggable
              aria-describedby="palette-hint"
              title="Drag onto the canvas, or click to add at the centre"
              className="flex size-full cursor-grab items-center gap-2.5 rounded-md border-[1.5px] border-border bg-surface px-3 text-left text-[13px] text-status hover:bg-muted focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary active:cursor-grabbing"
              onDragStart={(event) => {
                event.dataTransfer.setData(PALETTE_MIME, kind);
                event.dataTransfer.effectAllowed = 'copy';
              }}
              onClick={() => void addAtCentre(kind)}
            >
              <span aria-hidden="true" className="size-7 rounded bg-muted" />
              {label}
            </button>
          </li>
        ))}
      </ul>
    </aside>
  );
}
