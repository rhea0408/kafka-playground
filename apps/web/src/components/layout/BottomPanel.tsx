import * as Tabs from '@radix-ui/react-tabs';
import { EventList } from '../../events/EventList.tsx';
import { setBottomTab, useUiStore, type BottomTab } from '../../stores/ui-store.ts';
import { Placeholder } from './Placeholder.tsx';
import { TAB_TRIGGER } from './tab-styles.ts';

/** Tab order from ui-spec ("one tabbed component everywhere") and wireframes 02, 03, 05. */
const TABS: readonly { value: BottomTab; label: string }[] = [
  { value: 'timeline', label: 'Event timeline' },
  { value: 'events', label: 'Event list' },
  { value: 'lag', label: 'Lag chart' },
];

const isBottomTab = (value: string): value is BottomTab => TABS.some((t) => t.value === value);

/**
 * Bottom panel: canvas width × 200 with a 36 px tab bar (ui-spec, wireframe 05). The event list
 * is live (T11); the timeline and the lag chart arrive in M4 (M2-decisions T11).
 */
export function BottomPanel() {
  const tab = useUiStore((s) => s.bottomTab);
  return (
    <Tabs.Root
      value={tab}
      onValueChange={(value) => {
        if (isBottomTab(value)) setBottomTab(value);
      }}
      className="flex min-h-0 min-w-0 flex-col border-t-[1.5px] border-border bg-surface [grid-area:bottom]"
    >
      <Tabs.List
        aria-label="Bottom panel"
        className="flex h-9 shrink-0 items-stretch gap-5 border-b border-divider px-4"
      >
        {TABS.map((t) => (
          <Tabs.Trigger key={t.value} value={t.value} className={TAB_TRIGGER}>
            {t.label}
          </Tabs.Trigger>
        ))}
      </Tabs.List>
      <Tabs.Content value="timeline" className="min-h-0 flex-1 overflow-y-auto">
        <Placeholder>The event timeline arrives in M4.</Placeholder>
      </Tabs.Content>
      {/* The list scrolls itself (windowed); its filter panel may overlap the canvas above. */}
      <Tabs.Content value="events" className="min-h-0 flex-1">
        <EventList />
      </Tabs.Content>
      <Tabs.Content value="lag" className="min-h-0 flex-1 overflow-y-auto">
        <Placeholder>The lag chart arrives in M4.</Placeholder>
      </Tabs.Content>
    </Tabs.Root>
  );
}
