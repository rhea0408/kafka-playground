import { describe, expect, it } from 'vitest';
import { createKernel, type Kernel, type Stamped } from './kernel.ts';

type Task =
  { type: 'Ping'; name: string } | { type: 'Chain'; name: string; every: number; left: number };
type Ev = { type: 'Pinged'; name: string };

function setup(): { kernel: Kernel<Task, Ev>; log: Stamped<Ev>[] } {
  const log: Stamped<Ev>[] = [];
  const kernel: Kernel<Task, Ev> = createKernel<Task, Ev>({
    seed: 1,
    handleTask: (task) => {
      kernel.emit({ type: 'Pinged', name: task.name });
      if (task.type === 'Chain' && task.left > 1) {
        kernel.scheduleIn(task.every, { ...task, left: task.left - 1 });
      }
    },
  });
  kernel.on((e) => log.push(e));
  return { kernel, log };
}

describe('createKernel', () => {
  it('stamps events with sim time and a strictly increasing seq, payload after', () => {
    const { kernel, log } = setup();
    kernel.scheduleAt(100, { type: 'Ping', name: 'a' });
    kernel.scheduleAt(100, { type: 'Ping', name: 'b' });
    kernel.scheduleAt(250, { type: 'Ping', name: 'c' });
    kernel.runUntil(1000);
    expect(log).toEqual([
      { t: 100, seq: 0, type: 'Pinged', name: 'a' },
      { t: 100, seq: 1, type: 'Pinged', name: 'b' },
      { t: 250, seq: 2, type: 'Pinged', name: 'c' },
    ]);
    expect(Object.keys(log[0] ?? {})).toEqual(['t', 'seq', 'type', 'name']);
  });

  it('runUntil(t) runs tasks at exactly t, not after, and leaves the clock at t', () => {
    const { kernel, log } = setup();
    kernel.scheduleAt(499, { type: 'Ping', name: 'before' });
    kernel.scheduleAt(500, { type: 'Ping', name: 'at' });
    kernel.scheduleAt(501, { type: 'Ping', name: 'after' });
    kernel.runUntil(500);
    expect(log.map((e) => e.name)).toEqual(['before', 'at']);
    expect(kernel.now()).toBe(500);
    expect(kernel.pendingTasks()).toBe(1);
  });

  it('runUntil also runs tasks that are scheduled while it runs, if they fall inside the window', () => {
    const { kernel, log } = setup();
    kernel.scheduleAt(0, { type: 'Chain', name: 'tick', every: 500, left: 10 });
    kernel.runUntil(2000);
    expect(log.map((e) => e.t)).toEqual([0, 500, 1000, 1500, 2000]);
  });

  it('runUntil with an empty queue just moves the clock; it never goes backwards', () => {
    const { kernel } = setup();
    kernel.runUntil(1234);
    expect(kernel.now()).toBe(1234);
    expect(() => {
      kernel.runUntil(1000);
    }).toThrow(/backwards/);
  });

  it('step() processes one task, moves the clock to its time and counts steps', () => {
    const { kernel, log } = setup();
    kernel.scheduleAt(300, { type: 'Ping', name: 'a' });
    kernel.scheduleAt(300, { type: 'Ping', name: 'b' });
    expect(kernel.step()).toBe(true);
    expect(kernel.now()).toBe(300);
    expect(log.map((e) => e.name)).toEqual(['a']);
    expect(kernel.stepsProcessed()).toBe(1);
    expect(kernel.step()).toBe(true);
    expect(kernel.step()).toBe(false);
    expect(kernel.stepsProcessed()).toBe(2);
  });

  it('a zero-delay task runs later in the same instant, after already-queued work', () => {
    const { kernel, log } = setup();
    kernel.scheduleAt(100, { type: 'Chain', name: 'first', every: 0, left: 2 });
    kernel.scheduleAt(100, { type: 'Ping', name: 'second' });
    kernel.runUntil(100);
    expect(log.map((e) => e.name)).toEqual(['first', 'second', 'first']);
  });

  it('refuses to schedule in the past', () => {
    const { kernel } = setup();
    kernel.runUntil(1000);
    expect(() => {
      kernel.scheduleAt(999, { type: 'Ping', name: 'x' });
    }).toThrow(/past/);
  });

  it('advanceTo(t) keeps tasks due at t queued but refuses to skip earlier ones', () => {
    const { kernel, log } = setup();
    kernel.scheduleAt(500, { type: 'Ping', name: 'a' });
    kernel.advanceTo(500);
    expect(log).toEqual([]);
    expect(kernel.now()).toBe(500);
    expect(() => {
      kernel.advanceTo(501);
    }).toThrow(/due at 500/);
  });

  it('on() returns an unsubscribe function', () => {
    const { kernel } = setup();
    const seen: string[] = [];
    const off = kernel.on((e) => seen.push(e.name));
    kernel.scheduleAt(1, { type: 'Ping', name: 'a' });
    kernel.scheduleAt(2, { type: 'Ping', name: 'b' });
    kernel.step();
    off();
    kernel.step();
    expect(seen).toEqual(['a']);
  });
});
