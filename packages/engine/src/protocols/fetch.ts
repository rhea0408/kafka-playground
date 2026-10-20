/**
 * The broker side of consumer fetching: Kafka's long poll (Kafka docs, Design §"The Consumer":
 * "To avoid this we have parameters in our pull request that allow the consumer request to block
 * in a 'long poll' waiting until data arrives"; ConsumerConfig `fetch.min.bytes`,
 * `fetch.max.wait.ms`).
 *
 * A FetchRequest names partitions and the offset to read each from. The leader answers at once
 * when at least `fetch.min.bytes` are available below the high watermark over all requested
 * partitions, or when a partition has an error. Otherwise it parks the request (the broker's
 * DelayedFetch "purgatory") and answers as soon as enough data has been appended, or when
 * `fetch.max.wait.ms` has passed, with whatever is there (possibly nothing). The HW moving is what
 * wakes a parked fetch (Partition#tryCompleteDelayedRequests after the HW is incremented).
 *
 * Simplified: no fetch.max.bytes (50 MiB) or max.partition.fetch.bytes (1 MiB) limits; a response
 * carries every available record of each requested partition. No fetch sessions (KIP-227).
 */

import type { EngineContext } from '../model/context.ts';
import { bytesAvailable, readRecords, type PartitionState } from '../model/partition.ts';
import type { EngineTask, FetchPartitionResult, FetchRequestTask } from '../model/tasks.ts';

type Task<T extends EngineTask['type']> = Extract<EngineTask, { type: T }>;

/** A FetchRequest reaches the leader: answer now, or park it for up to maxWaitMs. */
export function handleFetchRequest(ctx: EngineContext, task: FetchRequestTask): void {
  if (task.maxWaitMs === 0 || canComplete(ctx, task)) {
    respond(ctx, task);
    return;
  }
  ctx.state.parkedFetches.set(task.fetchId, task);
  ctx.kernel.scheduleIn(task.maxWaitMs, { type: 'FetchExpire', fetchId: task.fetchId });
}

/** fetch.max.wait.ms passed: answer with what is available, which may be nothing. */
export function handleFetchExpire(ctx: EngineContext, task: Task<'FetchExpire'>): void {
  const parked = ctx.state.parkedFetches.get(task.fetchId);
  if (parked === undefined) return; // already answered when data arrived
  ctx.state.parkedFetches.delete(task.fetchId);
  respond(ctx, parked);
}

/** Called after a partition's HW moved: answers parked fetches that now have enough data. */
export function onHighWatermarkAdvanced(ctx: EngineContext, partition: PartitionState): void {
  for (const [fetchId, parked] of ctx.state.parkedFetches) {
    const touches = parked.partitions.some(
      (p) => p.topic === partition.topic && p.partition === partition.index,
    );
    if (!touches || !canComplete(ctx, parked)) continue;
    ctx.state.parkedFetches.delete(fetchId);
    respond(ctx, parked);
  }
}

/**
 * Called when a topic is deleted: parked fetches for it complete at once with
 * UNKNOWN_TOPIC_OR_PARTITION (ReplicaManager completes delayed fetches when it stops a replica).
 */
export function onTopicRemoved(ctx: EngineContext, topic: string): void {
  for (const [fetchId, parked] of ctx.state.parkedFetches) {
    if (!parked.partitions.some((p) => p.topic === topic)) continue;
    ctx.state.parkedFetches.delete(fetchId);
    respond(ctx, parked);
  }
}

function canComplete(ctx: EngineContext, task: FetchRequestTask): boolean {
  let bytes = 0;
  for (const result of readPartitions(ctx, task, false)) {
    if (result.error !== null) return true; // errors are returned at once
    bytes += result.bytes;
  }
  return bytes >= task.minBytes;
}

type PartitionRead = FetchPartitionResult & { readonly bytes: number };

/** Reads every requested partition; with `withRecords` false only the byte counts are needed. */
function readPartitions(
  ctx: EngineContext,
  task: FetchRequestTask,
  withRecords: boolean,
): PartitionRead[] {
  return task.partitions.map((p): PartitionRead => {
    const topic = ctx.state.topics.get(p.topic);
    const partition = topic?.partitions[p.partition];
    if (topic === undefined || partition === undefined) {
      return { ...p, topicId: null, error: 'UNKNOWN_TOPIC_OR_PARTITION', records: [], bytes: 0 };
    }
    const { topicId } = topic;
    // Valid fetch offsets run from the log start offset to the HW (the end for consumers).
    if (p.fetchOffset < partition.logStartOffset || p.fetchOffset > partition.highWatermark) {
      return { ...p, topicId, error: 'OFFSET_OUT_OF_RANGE', records: [], bytes: 0 };
    }
    const bytes = bytesAvailable(partition, p.fetchOffset);
    const records = withRecords ? readRecords(partition, p.fetchOffset) : [];
    return { ...p, topicId, error: null, records, bytes };
  });
}

/** Builds the response now (records read at this moment) and sends it back (+ network delay). */
function respond(ctx: EngineContext, task: FetchRequestTask): void {
  const partitions = readPartitions(ctx, task, true).map(
    ({ bytes: _bytes, ...result }): FetchPartitionResult => result,
  );
  ctx.kernel.scheduleIn(ctx.config.networkDelayMs, {
    type: 'FetchResponse',
    consumerId: task.consumerId,
    incarnation: task.incarnation,
    fetchId: task.fetchId,
    broker: task.broker,
    partitions,
  });
}
