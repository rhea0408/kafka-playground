/**
 * Connection rules for the canvas (FR-1.2), shared by the UI (to refuse a drag with a reason) and
 * the engine (to validate topologies and Connect commands), so both always agree.
 *
 * Data flows producer → topic → consumer. Brokers are never wired by hand: a topic's partitions
 * are placed on the cluster's brokers automatically.
 */

import type { ComponentKind } from './components.ts';

export type ConnectionCheck =
  { readonly ok: true } | { readonly ok: false; readonly reason: string };

const OK: ConnectionCheck = { ok: true };
const no = (reason: string): ConnectionCheck => ({ ok: false, reason });

/** Whether a `from` → `to` connection is allowed between these kinds of component. */
export function canConnect(from: ComponentKind, to: ComponentKind): ConnectionCheck {
  if (from === 'producer' && to === 'topic') return OK;
  if (from === 'topic' && to === 'consumer') return OK;

  if (from === 'broker' || to === 'broker') {
    return no(
      "Brokers aren't connected by hand. A topic's partitions are placed on the cluster's brokers automatically.",
    );
  }
  if (from === 'producer' && to === 'consumer') {
    return no(
      "A producer can't send straight to a consumer. Producers write to a topic, and consumers read from it.",
    );
  }
  if (from === 'topic' && to === 'producer') {
    return no('Data flows from producers into topics. Connect producer → topic instead.');
  }
  if (from === 'consumer' && to === 'topic') {
    return no('Consumers read from topics. Connect topic → consumer instead.');
  }
  if (from === 'consumer' && to === 'producer') {
    return no("A consumer can't feed a producer directly here. Connect the topic to the consumer.");
  }
  if (from === 'topic' && to === 'topic') {
    return no("One topic can't feed another directly. Something has to read it and write again.");
  }
  return no(`Two ${from}s can't be connected to each other.`);
}
