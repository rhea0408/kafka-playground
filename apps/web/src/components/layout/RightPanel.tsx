import type { ComponentKind } from '@kp/schema';
import * as Tabs from '@radix-ui/react-tabs';
import { useEffect } from 'react';
import { PartitionLog } from '../../partition-log/PartitionLog.tsx';
import { PropertiesPanel } from '../../properties/PropertiesPanel.tsx';
import { findComponent, useTopologyStore } from '../../stores/topology-store.ts';
import { setLogTarget, setRightTab, useUiStore, type RightTab } from '../../stores/ui-store.ts';
import { TAB_TRIGGER } from './tab-styles.ts';

const TABS: readonly { value: RightTab; label: string }[] = [
  { value: 'properties', label: 'Properties' },
  { value: 'partition-log', label: 'Partition log' },
];

const isRightTab = (value: string): value is RightTab => TABS.some((t) => t.value === value);

/**
 * FR-1.3: clicking a component opens its properties. Selecting one component switches to the
 * Properties tab, except that a topic selected while the Partition log is open keeps the log
 * (it shows that topic's partitions). Returns the tab to show, or null to keep the current one.
 */
export function tabOnSelect(current: RightTab, kind: ComponentKind | undefined): RightTab | null {
  if (kind === undefined || current === 'properties') return null;
  // Only the Partition log is left: it keeps a topic, anything else opens Properties.
  if (kind === 'topic') return null;
  return 'properties';
}

/**
 * Right panel, 360 px wide and full height below the top bar (ui-spec, wireframe 05), with a 40 px
 * tab bar. The Code tab from the wireframe arrives with codegen.
 */
export function RightPanel() {
  const tab = useUiStore((s) => s.rightTab);
  const selection = useUiStore((s) => s.selection);
  const [only] = selection.nodeIds;
  const selectedId = selection.nodeIds.length === 1 ? only : undefined;

  // Runs when the selection changes, not when the tab does, so the user can still switch tabs.
  useEffect(() => {
    if (selectedId === undefined) return;
    const kind = findComponent(useTopologyStore.getState(), selectedId)?.kind;
    // A selected topic becomes the Partition log's topic (05c), whichever tab is open.
    if (kind === 'topic' && useUiStore.getState().logTarget?.topicId !== selectedId) {
      setLogTarget({ topicId: selectedId, partition: 0 });
    }
    const next = tabOnSelect(useUiStore.getState().rightTab, kind);
    if (next !== null) setRightTab(next);
  }, [selectedId]);

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
        <PropertiesPanel />
      </Tabs.Content>
      {/* The log scrolls its own record list under a fixed picker and summary. */}
      <Tabs.Content value="partition-log" className="min-h-0 flex-1">
        <PartitionLog />
      </Tabs.Content>
    </Tabs.Root>
  );
}
