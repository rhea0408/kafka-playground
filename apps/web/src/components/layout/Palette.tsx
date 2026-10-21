/** The four component kinds M2 can build (T7 makes them draggable onto the canvas). */
const ITEMS = ['Producer', 'Topic', 'Broker', 'Consumer'] as const;

/**
 * Left palette, 240 px wide and full height below the top bar (ui-spec, wireframe 05). Items are
 * 208 × 56 shells for now: T7 adds drag and drop. They are not buttons until they act (ui-spec:
 * only things that act look like buttons), so they are a plain list without a hover state.
 */
export function Palette() {
  return (
    <aside
      aria-label="Palette"
      className="flex flex-col gap-2 overflow-y-auto border-r-[1.5px] border-border bg-surface p-4 [grid-area:palette]"
    >
      <h2 id="palette-components" className="mt-[18px] text-xs font-semibold text-status/70">
        COMPONENTS
      </h2>
      <ul aria-labelledby="palette-components" className="flex flex-col gap-2">
        {ITEMS.map((item) => (
          <li
            key={item}
            data-testid="palette-item"
            className="flex h-14 w-[208px] items-center gap-2.5 rounded-md border-[1.5px] border-border px-3 text-[13px]"
          >
            <span aria-hidden="true" className="size-7 rounded bg-muted" />
            {item}
          </li>
        ))}
      </ul>
    </aside>
  );
}
