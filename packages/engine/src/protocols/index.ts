/**
 * Runs one scheduled task. Each protocol (produce, consume) handles its own task types; this is
 * the single switch the kernel calls into.
 */

import { assertNever } from '../core/assert-never.ts';
import type { EngineContext } from '../model/context.ts';
import type { EngineTask } from '../model/tasks.ts';
import {
  handleConsumerCoordinatorResponse,
  handleConsumerPoll,
  handleFindCoordinatorResponse,
  handleFetchResponse,
  handleHeartbeatTask,
  handleJoinPrepareTimeoutTask,
  handlePollTimeoutTask,
} from './consume.ts';
import { handleFetchExpire, handleFetchRequest } from './fetch.ts';
import {
  handleCoordinatorRequest,
  handleFindCoordinatorRequest,
  handleGroupJoinTimer,
  handleGroupSyncTimer,
  handleMemberSessionExpire,
} from './group-coordinator.ts';
import {
  handleLeaderAppend,
  handleProduceResponse,
  handleProducerTask,
  handleProducerTick,
} from './produce.ts';
import {
  handleAlterPartitionRequest,
  handleBrokerHeartbeatRequest,
  handleBrokerHeartbeatTimer,
  handleBrokerRegistrationRequest,
  handleControllerSessionExpire,
  handlePreferredLeaderCheck,
} from './controller.ts';
import {
  handleBrokerRegistrationResponse,
  handleBrokerRegistrationRetry,
  handleBrokerShutdownAllowed,
} from '../commands/broker-lifecycle.ts';
import {
  handleAlterPartitionResponse,
  handleDelayedProduceExpire,
  handleReplicaFetchBackoff,
  handleReplicaFetchDisconnected,
  handleIsrExpirationCheck,
  handleReplicaFetchExpire,
  handleReplicaFetchRequest,
  handleReplicaFetchResponse,
} from './replication.ts';

export function runTask(ctx: EngineContext, task: EngineTask): void {
  switch (task.type) {
    case 'ProducerTick':
      handleProducerTick(ctx, task);
      return;
    case 'LeaderAppend':
      handleLeaderAppend(ctx, task);
      return;
    case 'ProduceResponse':
      handleProduceResponse(ctx, task);
      return;
    case 'ConsumerPoll':
      handleConsumerPoll(ctx, task);
      return;
    case 'FetchRequest':
      handleFetchRequest(ctx, task);
      return;
    case 'FetchExpire':
      handleFetchExpire(ctx, task);
      return;
    case 'ReplicaFetchRequest':
      handleReplicaFetchRequest(ctx, task);
      return;
    case 'ReplicaFetchExpire':
      handleReplicaFetchExpire(ctx, task);
      return;
    case 'ReplicaFetchResponse':
      handleReplicaFetchResponse(ctx, task);
      return;
    case 'ReplicaFetchDisconnected':
      handleReplicaFetchDisconnected(ctx, task);
      return;
    case 'ReplicaFetchBackoff':
      handleReplicaFetchBackoff(ctx, task);
      return;
    case 'IsrExpirationCheck':
      handleIsrExpirationCheck(ctx, task);
      return;
    case 'AlterPartitionRequest':
      handleAlterPartitionRequest(ctx, task);
      return;
    case 'AlterPartitionResponse':
      handleAlterPartitionResponse(ctx, task);
      return;
    case 'BrokerHeartbeatTimer':
      handleBrokerHeartbeatTimer(ctx, task);
      return;
    case 'BrokerHeartbeatRequest':
      handleBrokerHeartbeatRequest(ctx, task);
      return;
    case 'BrokerShutdownAllowed':
      handleBrokerShutdownAllowed(ctx, task);
      return;
    case 'BrokerRegistrationRequest':
      handleBrokerRegistrationRequest(ctx, task);
      return;
    case 'BrokerRegistrationResponse':
      handleBrokerRegistrationResponse(ctx, task);
      return;
    case 'BrokerRegistrationRetry':
      handleBrokerRegistrationRetry(ctx, task);
      return;
    case 'ControllerSessionExpire':
      handleControllerSessionExpire(ctx, task);
      return;
    case 'PreferredLeaderCheck':
      handlePreferredLeaderCheck(ctx);
      return;
    case 'ProducerDrain':
    case 'ProduceRequestTimeout':
    case 'ProduceDeliveryTimeout':
      handleProducerTask(ctx, task);
      return;
    case 'DelayedProduceExpire':
      handleDelayedProduceExpire(ctx, task);
      return;
    case 'FetchResponse':
      handleFetchResponse(ctx, task);
      return;
    case 'CoordinatorRequest':
      handleCoordinatorRequest(ctx, task);
      return;
    case 'FindCoordinatorRequest':
      handleFindCoordinatorRequest(ctx, task);
      return;
    case 'FindCoordinatorResponse':
      handleFindCoordinatorResponse(ctx, task);
      return;
    case 'CoordinatorResponse':
      handleConsumerCoordinatorResponse(ctx, task);
      return;
    case 'ConsumerHeartbeat':
      handleHeartbeatTask(ctx, task);
      return;
    case 'ConsumerPollTimeout':
      handlePollTimeoutTask(ctx, task);
      return;
    case 'ConsumerJoinPrepareTimeout':
      handleJoinPrepareTimeoutTask(ctx, task);
      return;
    case 'GroupJoinTimer':
      handleGroupJoinTimer(ctx, task);
      return;
    case 'GroupSyncTimer':
      handleGroupSyncTimer(ctx, task);
      return;
    case 'MemberSessionExpire':
      handleMemberSessionExpire(ctx, task);
      return;
    default:
      assertNever(task);
  }
}
