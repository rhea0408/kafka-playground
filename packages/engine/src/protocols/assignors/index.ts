/**
 * The client-side partition assignors (partition.assignment.strategy) and the rebalance protocol
 * they imply. See ADR 0004 and M3-decisions K2, K15, K16.
 */

import { rangeAssign } from './range.ts';
import { roundRobinAssign } from './roundrobin.ts';
import { cooperativeStickyAssign, stickyAssign } from './sticky.ts';
import type { AssignFn, AssignorName } from './types.ts';

export const ASSIGNORS: { readonly [N in AssignorName]: AssignFn } = {
  range: rangeAssign,
  roundrobin: roundRobinAssign,
  sticky: stickyAssign,
  'cooperative-sticky': cooperativeStickyAssign,
};

export type RebalanceProtocol = 'eager' | 'cooperative';

/**
 * The rebalance protocol a client uses: the highest one that *every* configured assignor supports
 * (ConsumerCoordinator constructor: "only consider protocols that are supported by all the
 * assignors"). Only CooperativeStickyAssignor supports COOPERATIVE, so the 4.x default
 * [range, cooperative-sticky] rebalances eagerly.
 */
export function rebalanceProtocol(strategies: readonly AssignorName[]): RebalanceProtocol {
  return strategies.length > 0 && strategies.every((s) => s === 'cooperative-sticky')
    ? 'cooperative'
    : 'eager';
}

export {
  comparePartitions,
  tpKey,
  type Assignment,
  type AssignFn,
  type AssignorMember,
  type PartitionCounts,
  type TopicPartition,
} from './types.ts';
