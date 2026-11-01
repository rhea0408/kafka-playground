/**
 * Properties of a consumer group (M3-T10, FR-2.9), shown when the group node's header is
 * selected: stat tiles, the assignor (applied to every member), a per-partition table like
 * `kafka-consumer-groups.sh --describe`, the members, and a chip when members subscribe to
 * different topics (Q6).
 */

import type { GroupSnapshot } from '@kp/engine';
import { useShallow } from 'zustand/react/shallow';
import { useActions } from '../actions/actions-context.tsx';
import { membersOf } from '../canvas/flow-mapping.ts';
import { formatCount, LAG_DANGER_THRESHOLD } from '../canvas/live/format.ts';
import { groupStateLabel } from '../canvas/live/live-parts.tsx';
import { useGroup } from '../canvas/live/use-live.ts';
import { useTopologyStore, type TopologyState } from '../stores/topology-store.ts';
import { focusMember } from '../stores/ui-store.ts';
import { PresetListField } from './controls.tsx';
import { FIELD_META } from './field-meta.ts';
import { StatTile } from './TopicSummary.tsx';

const SECTION_TITLE = 'text-[11px] font-bold text-status/70';
const NOTE = 'text-[12px] text-status/70';
const CHIP =
  'inline-flex items-center gap-1 rounded-sm bg-muted px-1.5 py-0.5 text-[11px] text-status';

/** Members' subscriptions (connected topics), and whether they differ (Q6). */
export function subscriptions(
  state: TopologyState,
  groupId: string,
): { readonly byMember: ReadonlyMap<string, readonly string[]>; readonly mismatch: boolean } {
  const byMember = new Map(
    membersOf(state, groupId).map((m) => [
      m.id,
      state.connections
        .filter((c) => c.to === m.id)
        .map((c) => c.from)
        .sort(),
    ]),
  );
  const shapes = new Set([...byMember.values()].map((topics) => topics.join(',')));
  return { byMember, mismatch: shapes.size > 1 };
}

const ASSIGNOR_META = FIELD_META.consumer.partitionAssignmentStrategy;

export function GroupPanel({ groupId }: { groupId: string }) {
  const actions = useActions();
  const group = useGroup(groupId);
  const members = useTopologyStore(useShallow((s) => membersOf(s, groupId)));
  // The whole store: it only changes on canvas edits, and subscriptions() builds a fresh Map.
  const topology = useTopologyStore();
  const subs = subscriptions(topology, groupId);
  if (members.length === 0) {
    return <p className="text-[13px] text-status/70">Select a component on the canvas.</p>;
  }
  const strategy = members[0]?.partitionAssignmentStrategy ?? [];
  const mixed = members.some((m) => m.partitionAssignmentStrategy.join(',') !== strategy.join(','));
  const state = groupStateLabel(group);
  const control = ASSIGNOR_META.control;
  return (
    <>
      <header className="mt-2.5">
        <div className="text-[11px] text-status/70">CONSUMER GROUP</div>
        <h2 className="truncate text-base font-bold">{groupId}</h2>
        <p className="mt-0.5 text-[12px]" data-testid="group-state">
          <span aria-hidden="true">{state.icon}</span> {state.text}
          {group?.protocol === null || group === undefined ? '' : ` · assignor ${group.protocol}`}
        </p>
      </header>
      <div className="grid grid-cols-[repeat(3,104px)] gap-2" aria-label="Group stats" role="group">
        <StatTile label="Members" value={String(members.length)} />
        <StatTile
          label="Generation"
          value={group === undefined ? '-' : String(group.generation)}
          title="Goes up by one each time a rebalance finishes."
        />
        <StatTile
          label="Total lag"
          value={group === undefined ? '-' : formatCount(group.totalLag)}
          title="Sum over the group's partitions of HW − committed offset (kafka-consumer-groups.sh LAG)."
        />
      </div>
      {subs.mismatch && (
        <p className={CHIP} data-slot="subscription-mismatch" role="note">
          <span aria-hidden="true">⚠&#xFE0E;</span>
          Members read different topics. Kafka allows it, but range assignment then looks uneven.
        </p>
      )}
      <section aria-labelledby="pp-assignor" className="flex flex-col gap-1">
        <label id="pp-assignor" htmlFor="pp-group-assignor" className={SECTION_TITLE}>
          ASSIGNMENT STRATEGY (EVERY MEMBER)
        </label>
        <PresetListField
          inputId="pp-group-assignor"
          value={strategy}
          presets={control.presets}
          describedBy="pp-group-assignor-help"
          invalid={false}
          commit={(value) => {
            for (const m of members) {
              void actions.updateConfig(m.id, {
                partitionAssignmentStrategy: value as typeof strategy,
              });
            }
          }}
        />
        <p id="pp-group-assignor-help" className={NOTE}>
          {mixed ? 'Members have different lists now. ' : ''}
          {ASSIGNOR_META.help} Kafka default: range, cooperative-sticky.
        </p>
      </section>
      <PartitionTable group={group} />
      <section aria-labelledby="pp-members" className="flex flex-col gap-1">
        <h3 id="pp-members" className={SECTION_TITLE}>
          MEMBERS
        </h3>
        <ul className="flex flex-col gap-0.5 text-[13px]">
          {members.map((m) => (
            <li key={m.id}>
              <button
                type="button"
                className="rounded px-1 font-mono underline-offset-2 hover:underline focus-visible:outline-2 focus-visible:outline-primary"
                onClick={() => {
                  focusMember(m.id);
                }}
              >
                {m.id}
              </button>
              <span className={NOTE}>
                {' '}
                · reads {(subs.byMember.get(m.id) ?? []).join(', ') || 'nothing'}
              </span>
            </li>
          ))}
        </ul>
        <p className={NOTE}>Click a member (here or its row on the canvas) to edit its config.</p>
      </section>
    </>
  );
}

const dash = (n: number | null): string => (n === null ? '-' : formatCount(n));

/** Owner, committed offset, LEO and lag per partition (FR-2.9). */
function PartitionTable({ group }: { group: GroupSnapshot | undefined }) {
  const rows = group?.partitions ?? [];
  return (
    <section aria-labelledby="pp-group-partitions" className="flex flex-col gap-1">
      <h3 id="pp-group-partitions" className={SECTION_TITLE}>
        PARTITIONS
      </h3>
      {rows.length === 0 ? (
        <p className={NOTE}>The group reads no partition yet.</p>
      ) : (
        <table className="w-[328px] text-left font-mono text-[12px]" data-testid="group-partitions">
          <thead>
            <tr className="h-6 text-[11px] text-status/70">
              <th className="font-normal">partition</th>
              <th className="font-normal">owner</th>
              <th className="text-right font-normal">committed</th>
              <th className="text-right font-normal">LEO</th>
              <th className="text-right font-normal">lag</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((p) => {
              const danger = p.lag !== null && p.lag > LAG_DANGER_THRESHOLD;
              return (
                <tr key={`${p.topic}-${String(p.partition)}`} className="h-6">
                  <td>
                    {p.topic}-{p.partition}
                  </td>
                  <td>{p.owner ?? '-'}</td>
                  <td className="text-right">{dash(p.committed)}</td>
                  <td className="text-right">{formatCount(p.logEndOffset)}</td>
                  <td className={`text-right ${danger ? 'text-danger-text' : ''}`}>
                    {dash(p.lag)}
                    {danger && ' ⚠︎'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </section>
  );
}
