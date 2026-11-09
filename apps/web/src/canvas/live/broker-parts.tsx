/**
 * Live parts for brokers and the KRaft controller (M4-T11): the broker's state (icon + word, never
 * color alone), its replica manager counts, the groups it coordinates, the A6 failed-broker look,
 * the Kill / Stop / Restart hover toolbar, and the controller chip.
 */

import type { BrokerSnapshot } from '@kp/engine';
import { NodeToolbar, Position } from '@xyflow/react';
import { useActions } from '../../actions/actions-context.tsx';
import { useUiStore } from '../../stores/ui-store.ts';
import type { HoverToolbar } from './live-parts.tsx';
import { useBroker, useController } from './use-live.ts';

const STATE_LABEL: { readonly [S in BrokerSnapshot['state']]: string } = {
  running: 'Running',
  fenced: 'Fenced',
  'shutting-down': 'Shutting down',
  stopped: 'Stopped',
  crashed: 'Crashed',
};

const STATE_ICON: { readonly [S in BrokerSnapshot['state']]: string } = {
  running: '●',
  fenced: '⊘',
  'shutting-down': '◐',
  stopped: '■',
  crashed: '✕',
};

const STATE_HELP: { readonly [S in BrokerSnapshot['state']]: string } = {
  running: 'Up, and registered with the KRaft controller: it can lead partitions and be in sync.',
  fenced:
    'Up, but fenced by the controller: it can’t lead or be in an ISR yet (a restarted broker until its first heartbeat, or one whose registration waits for its old session to expire).',
  'shutting-down': 'Controlled shutdown: the controller is moving its leaderships away.',
  stopped: 'Stopped cleanly. Its log is on disk; Restart brings it back as a follower.',
  crashed:
    'Crashed. The controller notices only when broker.session.timeout.ms (9 s) passes without a heartbeat, then fences it and elects new leaders.',
};

export const isDown = (state: BrokerSnapshot['state'] | undefined): boolean =>
  state === 'crashed' || state === 'stopped';

/** The body of a broker node: state, leads / follows, coordinator chip. */
export function BrokerLive({ brokerId }: { brokerId: string }) {
  const broker = useBroker(brokerId);
  if (broker === undefined) return null;
  const danger = isDown(broker.state) || broker.state === 'fenced';
  return (
    <div className="flex flex-col gap-0.5" data-broker-state={broker.state}>
      <span
        className={`inline-flex w-fit items-center gap-1 rounded-sm px-1 text-[11px] font-medium ${
          danger ? 'bg-danger-bg text-danger-text' : 'bg-muted'
        }`}
        data-slot="broker-state"
        title={STATE_HELP[broker.state]}
        tabIndex={0}
      >
        <span aria-hidden="true">{STATE_ICON[broker.state]}</span>
        {STATE_LABEL[broker.state]}
      </span>
      <span className="text-[11px] text-status/80" data-slot="broker-replicas">
        Leads {broker.partitionsLed} · follows {broker.partitionsFollowed}
      </span>
      {broker.coordinatorFor.length > 0 && (
        <span
          className="truncate text-[10px] text-status/70"
          data-slot="coordinator"
          title={`Group coordinator for ${broker.coordinatorFor.join(', ')}: it leads their __consumer_offsets partition.`}
        >
          Coordinator: {broker.coordinatorFor.join(', ')}
        </span>
      )}
    </div>
  );
}

/** A6: class names for the broker frame (hatched while down, pulse ring while crashed). */
export function useBrokerFrameClass(brokerId: string): string {
  const broker = useBroker(brokerId);
  const reduced = useUiStore((s) => s.reducedMotion);
  if (broker === undefined || !isDown(broker.state)) return '';
  return `kp-broker-down ${broker.state === 'crashed' && !reduced ? 'kp-broker-pulse' : ''}`;
}

const TOOL_BUTTON =
  'flex h-7 items-center gap-1.5 rounded px-2 text-xs text-surface hover:bg-surface/15 focus-visible:outline-2 focus-visible:outline-surface';

/** What each action does, for its tooltip (M4 Q3): the difference is the lesson. */
export const BROKER_ACTION_HELP = {
  kill: 'Crash the broker process. Requests in flight are lost; the controller only notices after broker.session.timeout.ms (9 s), then elects new leaders from the in-sync replicas.',
  stop: 'Controlled shutdown: the controller moves this broker’s leaderships to other in-sync replicas first, then it stops. Nothing is lost and there is no gap in leadership.',
  restart:
    'Start the broker again with its log on disk. It registers, gets unfenced, truncates records the new leaders don’t have, catches up and rejoins the ISR.',
} as const;

/** The broker node's hover toolbar (200 × 36, ui-spec): Kill (crash), Stop (shutdown), Restart. */
export function BrokerToolbar({ brokerId, toolbar }: { brokerId: string; toolbar: HoverToolbar }) {
  const actions = useActions();
  const broker = useBroker(brokerId);
  const down = isDown(broker?.state);
  return (
    <NodeToolbar isVisible={toolbar.visible} position={Position.Top} offset={6}>
      <div
        ref={toolbar.toolbarRef}
        role="toolbar"
        aria-label={`Actions for broker ${brokerId}`}
        className="nodrag nopan flex h-9 items-center gap-1 rounded-md bg-status px-1"
        onPointerEnter={toolbar.onPointerEnter}
        onPointerLeave={toolbar.onPointerLeave}
        onFocus={toolbar.onToolbarFocus}
        onBlur={toolbar.onToolbarBlur}
        onKeyDown={toolbar.onToolbarKeyDown}
        onClick={(event) => {
          event.stopPropagation();
        }}
      >
        {down ? (
          <button
            type="button"
            className={TOOL_BUTTON}
            title={BROKER_ACTION_HELP.restart}
            aria-label={`Restart broker ${brokerId}`}
            onClick={() => {
              void actions.restartBroker(brokerId);
            }}
          >
            <span aria-hidden="true">▶</span>Restart
          </button>
        ) : (
          <>
            <button
              type="button"
              className={TOOL_BUTTON}
              title={BROKER_ACTION_HELP.kill}
              aria-label={`Kill broker ${brokerId}`}
              onClick={() => {
                void actions.killBroker(brokerId);
              }}
            >
              <span aria-hidden="true">✕</span>Kill (crash)
            </button>
            <button
              type="button"
              className={TOOL_BUTTON}
              title={BROKER_ACTION_HELP.stop}
              aria-label={`Stop broker ${brokerId}`}
              disabled={broker?.state === 'shutting-down'}
              onClick={() => {
                void actions.stopBroker(brokerId);
              }}
            >
              <span aria-hidden="true">■</span>Stop (shutdown)
            </button>
          </>
        )}
      </div>
    </NodeToolbar>
  );
}

/**
 * The KRaft controller chip (Q2): not a broker, always up. Shows what the controller knows:
 * fenced brokers, offline and under-replicated partitions, and its (read-only) timers.
 */
export function ControllerChip() {
  const controller = useController();
  if (controller === undefined) return null;
  const { fencedBrokers, offlinePartitions, underReplicatedPartitions } = controller;
  const trouble = offlinePartitions > 0 || fencedBrokers.length > 0;
  return (
    <div
      className={`flex flex-col gap-0.5 rounded-md border bg-surface px-2 py-1.5 text-[11px] text-status shadow-sm ${
        trouble ? 'border-danger' : 'border-divider'
      }`}
      data-testid="controller-chip"
      role="group"
      aria-label="KRaft controller"
    >
      <span className="font-semibold">KRaft controller</span>
      <span
        title="Brokers heartbeat every broker.heartbeat.interval.ms; one silent for broker.session.timeout.ms is fenced. Real Kafka defaults, read-only."
        className="text-status/70"
      >
        session 9 s · heartbeat 2 s
      </span>
      <span data-slot="fenced">
        Fenced: {fencedBrokers.length === 0 ? 'none' : fencedBrokers.join(', ')}
      </span>
      <span className="flex flex-wrap gap-1">
        {offlinePartitions > 0 && (
          <span
            className="rounded-sm bg-danger-bg px-1 text-danger-text"
            data-slot="offline"
            title="Partitions with no leader: producers and consumers wait until one is elected."
          >
            ⚠ {offlinePartitions} offline
          </span>
        )}
        {underReplicatedPartitions > 0 && (
          <span
            className="rounded-sm bg-muted px-1"
            data-slot="under-replicated"
            title="Partitions whose ISR is smaller than their replica list."
          >
            {underReplicatedPartitions} under-replicated
          </span>
        )}
      </span>
    </div>
  );
}
