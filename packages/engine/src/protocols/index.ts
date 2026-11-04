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
  handleFetchResponse,
  handleHeartbeatTask,
  handleJoinPrepareTimeoutTask,
  handlePollTimeoutTask,
} from './consume.ts';
import { handleFetchExpire, handleFetchRequest } from './fetch.ts';
import {
  handleCoordinatorRequest,
  handleGroupJoinTimer,
  handleGroupSyncTimer,
  handleMemberSessionExpire,
} from './group-coordinator.ts';
import { handleLeaderAppend, handleProduceResponse, handleProducerTick } from './produce.ts';

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
    case 'FetchResponse':
      handleFetchResponse(ctx, task);
      return;
    case 'CoordinatorRequest':
      handleCoordinatorRequest(ctx, task);
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
