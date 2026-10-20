/**
 * Runs one scheduled task. Each protocol (produce, consume) handles its own task types; this is
 * the single switch the kernel calls into.
 */

import { assertNever } from '../core/assert-never.ts';
import type { EngineContext } from '../model/context.ts';
import type { EngineTask } from '../model/tasks.ts';

export function runTask(_ctx: EngineContext, task: EngineTask): void {
  assertNever(task);
}
