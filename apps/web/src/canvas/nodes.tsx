/**
 * Custom React Flow nodes at the ui-spec sizes ("Canvas node sizes", wireframe 05). The node shell
 * shows the config (it re-renders only when topology changes); the live parts from
 * `live/live-parts.tsx` subscribe to the sim snapshot themselves, so a frame re-renders only the
 * parts whose data changed (M2-T8).
 */

import type { KeyStrategy } from '@kp/schema';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { useState, type ReactNode } from 'react';
import { focusMember, useUiStore } from '../stores/ui-store.ts';
import {
  ConsumerLive,
  ConsumerToolbar,
  GroupHeaderLive,
  GroupHeaderStats,
  PartitionLive,
  PartitionStatus,
  ProducerCounters,
  ProducerRetrying,
  useHoverToolbar,
} from './live/live-parts.tsx';
import { BrokerLive, BrokerToolbar, useBrokerFrameClass } from './live/broker-parts.tsx';
import type {
  BrokerFlowNode,
  ConsumerGroupFlowNode,
  ProducerFlowNode,
  TopicFlowNode,
} from './flow-mapping.ts';
import {
  BROKER_SIZE,
  consumerRowHandleId,
  GROUP_HEADER_CENTER_Y,
  GROUP_HEADER_HEIGHT,
  GROUP_ROW_HEIGHT,
  GROUP_ROW_WIDTH,
  GROUP_WIDTH,
  groupHeight,
  groupRowCenterY,
  HANDLE_IN,
  HANDLE_OUT,
  partitionInAnchorId,
  partitionOutAnchorId,
  partitionRowCenterY,
  PRODUCER_SIZE,
  TOPIC_HEADER_HEIGHT,
  TOPIC_ROW_GAP,
  TOPIC_ROW_HEIGHT,
  TOPIC_STATUS_LINE_HEIGHT,
  TOPIC_WIDTH,
  topicHeight,
} from './geometry.ts';

/** Selected: thicker border plus a dashed ring (wireframe 05), so it is not shown by color alone. */
function frameClass(selected: boolean): string {
  return `relative rounded-lg bg-surface text-status ${
    selected
      ? 'border-[2.5px] border-status outline-2 outline-offset-4 outline-status outline-dashed'
      : 'border-[1.5px] border-status/70'
  }`;
}

/** Connectable handle; 10 px so it is easy to grab. */
const HANDLE_CLASS = 'kp-handle';
/** Invisible anchor React Flow still measures (for the overlay); not connectable. */
const ANCHOR_CLASS = 'kp-anchor';

export function keySummary(strategy: KeyStrategy): string {
  switch (strategy.type) {
    case 'null':
      return 'no key';
    case 'fixed':
      return `key ${strategy.key}`;
    case 'random':
      return strategy.keys.length <= 3
        ? `keys ${strategy.keys.join(', ')}`
        : `${String(strategy.keys.length)} keys`;
    case 'skewed':
      return `hot key ${strategy.hotKey}`;
  }
}

function Muted({ children }: { children: ReactNode }) {
  return <div className="truncate text-[11px] text-status/70">{children}</div>;
}

export function ProducerNode({ data, selected }: NodeProps<ProducerFlowNode>) {
  const p = data.component;
  return (
    <div
      className={`${frameClass(selected)} px-3 py-2`}
      style={{ width: PRODUCER_SIZE.width, height: PRODUCER_SIZE.height }}
      data-kind="producer"
    >
      <div className="flex items-center gap-1.5 leading-4">
        <span className="truncate text-[13px] font-semibold">{p.id}</span>
        <span className="ml-auto shrink-0 rounded-sm bg-muted px-1 font-mono text-[10px]">
          acks={p.acks}
        </span>
      </div>
      <Muted>
        {String(p.ratePerSec)} msg/s · {keySummary(p.keyStrategy)}
      </Muted>
      <ProducerCounters producerId={p.id} />
      <ProducerRetrying producerId={p.id} />
      <Handle
        type="source"
        position={Position.Right}
        id={HANDLE_OUT}
        className={HANDLE_CLASS}
        aria-label={`Output of ${p.id}: drag to a topic`}
      />
    </div>
  );
}

export function TopicNode({ data, selected }: NodeProps<TopicFlowNode>) {
  const t = data.component;
  const rows = Array.from({ length: t.partitions }, (_, i) => i);
  return (
    <div
      className={`${frameClass(selected)} px-3`}
      style={{ width: TOPIC_WIDTH, height: topicHeight(t.partitions, t.replicationFactor) }}
      data-kind="topic"
    >
      <div
        className="flex items-center gap-1.5 border-b border-divider text-[13px] font-semibold"
        style={{ height: TOPIC_HEADER_HEIGHT }}
      >
        <span className="truncate">Topic: {t.id}</span>
        <span className="shrink-0 text-[11px] font-normal text-status/70">
          {t.partitions} {t.partitions === 1 ? 'partition' : 'partitions'} · RF{' '}
          {t.replicationFactor}
        </span>
      </div>
      {rows.map((i) => (
        <div key={i} data-partition-row={i} style={{ marginTop: TOPIC_ROW_GAP }}>
          <div
            className="flex items-center gap-1.5 text-[11px]"
            style={{ height: TOPIC_ROW_HEIGHT }}
          >
            <span className="w-[22px] shrink-0 font-medium">P{i}</span>
            <PartitionLive topicId={t.id} index={i} />
          </div>
          {t.replicationFactor > 1 && (
            <div
              className="flex items-center justify-end gap-1 pl-7"
              style={{ height: TOPIC_STATUS_LINE_HEIGHT }}
              data-slot="partition-status"
            >
              <PartitionStatus topicId={t.id} index={i} />
            </div>
          )}
        </div>
      ))}
      <Handle
        type="target"
        position={Position.Left}
        id={HANDLE_IN}
        className={HANDLE_CLASS}
        aria-label={`Input of topic ${t.id}: drop a producer connection here`}
      />
      <Handle
        type="source"
        position={Position.Right}
        id={HANDLE_OUT}
        className={HANDLE_CLASS}
        aria-label={`Output of topic ${t.id}: drag to a consumer`}
      />
      {rows.map((i) => (
        <PartitionAnchors key={i} index={i} replicationFactor={t.replicationFactor} />
      ))}
    </div>
  );
}

/** Per-partition anchors at the row's left and right edges, for the message overlay (T9). */
function PartitionAnchors({
  index,
  replicationFactor,
}: {
  index: number;
  replicationFactor: number;
}) {
  const top = partitionRowCenterY(index, replicationFactor);
  return (
    <>
      <Handle
        type="target"
        position={Position.Left}
        id={partitionInAnchorId(index)}
        isConnectable={false}
        className={ANCHOR_CLASS}
        style={{ top }}
        aria-hidden="true"
      />
      <Handle
        type="source"
        position={Position.Right}
        id={partitionOutAnchorId(index)}
        isConnectable={false}
        className={ANCHOR_CLASS}
        style={{ top }}
        aria-hidden="true"
      />
    </>
  );
}

/**
 * A broker (M4-T11): its state (icon + word), how many partitions it leads and follows, the groups
 * it coordinates; hatched while down (A6). Hovering shows Kill / Stop / Restart.
 */
export function BrokerNode({ data, selected }: NodeProps<BrokerFlowNode>) {
  const b = data.component;
  const toolbar = useHoverToolbar(selected);
  const down = useBrokerFrameClass(b.id);
  return (
    <div
      ref={toolbar.rootRef}
      className={`${frameClass(selected)} ${down} rounded-md px-2.5 py-1.5 text-xs`}
      style={{ width: BROKER_SIZE.width, height: BROKER_SIZE.height }}
      data-kind="broker"
      onPointerEnter={toolbar.onPointerEnter}
      onPointerLeave={toolbar.onPointerLeave}
    >
      <div className="flex items-baseline gap-1.5">
        <strong>Broker {b.id}</strong>
        <Muted>KRaft{b.rack === null ? '' : ` · rack ${b.rack}`}</Muted>
      </div>
      <BrokerLive brokerId={b.id} />
      <BrokerToolbar brokerId={b.id} toolbar={toolbar} />
    </div>
  );
}

/**
 * A consumer group (M3-T8, ui-spec "Consumer group", wireframe 05): the header shows the group id,
 * the coordinator's state and the generation; each member has a 196 × 40 row with its status, the
 * partitions it owns and its lag. Topics connect to the group's input on the header; A2 dots end
 * on the row of the member that read the record. Clicking a row focuses that member (properties,
 * Delete); clicking the header focuses the group.
 */
export function ConsumerGroupNode({ data, selected }: NodeProps<ConsumerGroupFlowNode>) {
  const toolbar = useHoverToolbar(selected);
  // The toolbar acts on the member row under the pointer, else the focused one, else the first.
  const [hoveredMember, setHoveredMember] = useState<string | null>(null);
  const focused = useUiStore((s) => s.focusedMember);
  const ids = data.members.map((m) => m.id);
  const active = [hoveredMember, focused].find((id) => id !== null && ids.includes(id)) ?? ids[0];
  return (
    <div
      ref={toolbar.rootRef}
      className={`${frameClass(selected)} px-3`}
      style={{ width: GROUP_WIDTH, height: groupHeight(data.members.length) }}
      data-kind="consumer"
      onPointerEnter={toolbar.onPointerEnter}
      onPointerLeave={toolbar.onPointerLeave}
    >
      <ConsumerToolbar consumerId={active ?? undefined} toolbar={toolbar} />
      <div
        className="flex flex-col justify-center border-b border-divider"
        style={{ height: GROUP_HEADER_HEIGHT }}
        data-slot="group-header"
        onClick={() => {
          focusMember(null);
        }}
      >
        <div className="flex min-w-0 items-center gap-1.5 text-[13px] leading-4 font-semibold">
          <span className="min-w-0 truncate" title={`Consumer group ${data.groupId}`}>
            <span className="sr-only">Group: </span>
            {data.groupId}
          </span>
          <GroupHeaderLive groupId={data.groupId} />
        </div>
        <div className="leading-3">
          <GroupHeaderStats groupId={data.groupId} />
        </div>
      </div>
      <Handle
        type="target"
        position={Position.Left}
        id={HANDLE_IN}
        className={HANDLE_CLASS}
        style={{ top: GROUP_HEADER_CENTER_Y }}
        aria-label={`Input of consumer group ${data.groupId}: drop a topic connection here to subscribe every member`}
      />
      {data.members.map((consumer, i) => (
        <div
          key={consumer.id}
          data-consumer-row={consumer.id}
          className="kp-consumer-row mt-2 flex items-center rounded-md border border-border px-2 text-xs"
          style={{ width: GROUP_ROW_WIDTH, height: GROUP_ROW_HEIGHT }}
          onClick={() => {
            focusMember(consumer.id);
          }}
          onPointerEnter={() => {
            setHoveredMember(consumer.id);
          }}
        >
          <ConsumerLive consumerId={consumer.id} />
          <Handle
            type="target"
            position={Position.Left}
            id={consumerRowHandleId(consumer.id)}
            isConnectable={false}
            className={ANCHOR_CLASS}
            style={{ top: groupRowCenterY(i) }}
            aria-hidden="true"
          />
        </div>
      ))}
    </div>
  );
}

export const NODE_TYPES = {
  producer: ProducerNode,
  topic: TopicNode,
  broker: BrokerNode,
  consumerGroup: ConsumerGroupNode,
};
