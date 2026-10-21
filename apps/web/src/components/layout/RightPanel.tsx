import * as Tabs from '@radix-ui/react-tabs';
import { setRightTab, useUiStore, type RightTab } from '../../stores/ui-store.ts';
import { Placeholder } from './Placeholder.tsx';
import { TAB_TRIGGER } from './tab-styles.ts';

const TABS: readonly { value: RightTab; label: string }[] = [
  { value: 'properties', label: 'Properties' },
  { value: 'partition-log', label: 'Partition log' },
];

const isRightTab = (value: string): value is RightTab => TABS.some((t) => t.value === value);

/**
 * Right panel, 360 px wide and full height below the top bar (ui-spec, wireframe 05), with a 40 px
 * tab bar. The Code tab from the wireframe arrives with codegen; the contents arrive in T10/T11.
 */
export function RightPanel() {
  const tab = useUiStore((s) => s.rightTab);
  return (
    <Tabs.Root
      value={tab}
      onValueChange={(value) => {
        if (isRightTab(value)) setRightTab(value);
      }}
      className="flex min-h-0 flex-col border-l-[1.5px] border-border bg-surface [grid-area:right]"
    >
      <Tabs.List
        aria-label="Right panel"
        className="flex h-10 shrink-0 items-stretch gap-5 border-b border-divider px-4"
      >
        {TABS.map((t) => (
          <Tabs.Trigger key={t.value} value={t.value} className={TAB_TRIGGER}>
            {t.label}
          </Tabs.Trigger>
        ))}
      </Tabs.List>
      <Tabs.Content value="properties" className="min-h-0 flex-1 overflow-y-auto">
        <Placeholder>Select a component on the canvas to see its properties.</Placeholder>
      </Tabs.Content>
      <Tabs.Content value="partition-log" className="min-h-0 flex-1 overflow-y-auto">
        <Placeholder>Select a topic to see its partition log.</Placeholder>
      </Tabs.Content>
    </Tabs.Root>
  );
}
