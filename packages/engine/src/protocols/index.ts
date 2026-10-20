/**
 * Runs one scheduled task. Each protocol (produce, consume) handles its own task types; this is
 * the single switch the kernel calls into.
 */

import { assertNever } from '../core/assert-never.ts';
import type { EngineContext } from '../model/context.ts';
import type { EngineTask } from '../model/tasks.ts';
import { handleConsumerPoll } from './consume.ts';
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
    default:
      assertNever(task);
  }
}
