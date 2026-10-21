/**
 * Custom React Flow nodes at the ui-spec sizes ("Canvas node sizes", wireframe 05). They show the
 * static config only; T8 adds live state (log cells, offsets, lag, producer stats) into the slots
 * marked below.
 */

import type { KeyStrategy } from '@kp/schema';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { ReactNode } from 'react';
import type {
  BrokerFlowNode,
  ConsumerGroupFlowNode,
  ProducerFlowNode,
  TopicFlowNode,
} from './flow-mapping.ts';
import {
  BROKER_SIZE,
  consumerRowHandleId,
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
      className={`${frameClass(selected)} px-3 py-2.5`}
      style={{ width: PRODUCER_SIZE.width, height: PRODUCER_SIZE.height }}
      data-kind="producer"
    >
      <div className="truncate text-[13px] font-semibold">{p.id}</div>
      <Muted>
        {String(p.ratePerSec)} msg/s · {keySummary(p.keyStrategy)}
      </Muted>
      <Muted>acks={p.acks}</Muted>
      {/* T8: sent / acked / failed counters. */}
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
      style={{ width: TOPIC_WIDTH, height: topicHeight(t.partitions) }}
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
        <div
          key={i}
          data-partition-row={i}
          className="flex items-center gap-1.5 text-[11px]"
          style={{ height: TOPIC_ROW_HEIGHT, marginTop: TOPIC_ROW_GAP }}
        >
          <span className="w-[22px] font-medium">P{i}</span>
          {/* T8: LEO / HW, the last log cells (18 × 18) and the leader chip go here. */}
          <span className="font-mono text-[10px] text-status/70" data-slot="partition-log" />
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
        <PartitionAnchors key={i} index={i} />
      ))}
    </div>
  );
}

/** Per-partition anchors at the row's left and right edges, for the message overlay (T9). */
function PartitionAnchors({ index }: { index: number }) {
  const top = partitionRowCenterY(index);
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

export function BrokerNode({ data, selected }: NodeProps<BrokerFlowNode>) {
  const b = data.component;
  return (
    <div
      className={`${frameClass(selected)} rounded-md px-2.5 py-2 text-xs`}
      style={{ width: BROKER_SIZE.width, height: BROKER_SIZE.height }}
      data-kind="broker"
    >
      <div>
        <strong>Broker {b.id}</strong>
      </div>
      {/* T8: up / down state (icon + label) and the partitions it leads. */}
      <Muted>KRaft · rack {b.rack ?? 'not set'}</Muted>
      <Muted>Partitions placed automatically</Muted>
    </div>
  );
}

export function ConsumerGroupNode({ data, selected }: NodeProps<ConsumerGroupFlowNode>) {
  return (
    <div
      className={`${frameClass(selected)} px-3`}
      style={{ width: GROUP_WIDTH, height: groupHeight(data.members.length) }}
      data-kind="consumer"
    >
      <div
        className="flex items-center border-b border-divider text-[13px] font-semibold"
        style={{ height: GROUP_HEADER_HEIGHT }}
      >
        <span className="truncate">Group: {data.groupId}</span>
      </div>
      {data.members.map((consumer, i) => (
        <div
          key={consumer.id}
          data-consumer-row={consumer.id}
          className="mt-2 flex items-center gap-1.5 rounded-md border border-border px-2 text-xs"
          style={{ width: GROUP_ROW_WIDTH, height: GROUP_ROW_HEIGHT }}
        >
          <span className="truncate font-medium">{consumer.id}</span>
          <span className="truncate text-[11px] text-status/70">
            reset: {consumer.autoOffsetReset}
          </span>
          {/* T8: status (icon + label), position, committed offset and the lag badge. */}
          <Handle
            type="target"
            position={Position.Left}
            id={consumerRowHandleId(consumer.id)}
            className={HANDLE_CLASS}
            style={{ top: groupRowCenterY(i) }}
            aria-label={`Input of consumer ${consumer.id}: drop a topic connection here`}
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
