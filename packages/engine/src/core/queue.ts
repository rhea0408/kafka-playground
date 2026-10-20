/**
 * Priority queue of scheduled engine work, ordered by `(time, seq)` (architecture §4.1).
 *
 * `seq` is a counter assigned on push, so two items scheduled for the same sim time come out in
 * the order they were scheduled. Without it a binary heap would return equal-time items in an
 * order that depends on the heap's internal layout, and two otherwise identical runs could
 * diverge. Because no two items share a seq, `(time, seq)` is a total order.
 */

import { assertSimTime, type SimTime } from './clock.ts';

export interface Scheduled<T> {
  readonly time: SimTime;
  readonly seq: number;
  readonly item: T;
}

export interface EventQueue<T> {
  /** Schedules `item` at `time` and returns the entry with its assigned seq. */
  readonly push: (time: SimTime, item: T) => Scheduled<T>;
  /** Removes and returns the earliest entry, or undefined when empty. */
  readonly pop: () => Scheduled<T> | undefined;
  /** Returns the earliest entry without removing it. */
  readonly peek: () => Scheduled<T> | undefined;
  readonly size: number;
}

export function createEventQueue<T>(): EventQueue<T> {
  const heap: Scheduled<T>[] = [];
  let nextSeq = 0;

  const at = (i: number): Scheduled<T> => heap[i] as Scheduled<T>;

  const before = (x: Scheduled<T>, y: Scheduled<T>): boolean =>
    x.time < y.time || (x.time === y.time && x.seq < y.seq);

  const swap = (i: number, j: number): void => {
    const tmp = at(i);
    heap[i] = at(j);
    heap[j] = tmp;
  };

  const siftUp = (start: number): void => {
    let i = start;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (!before(at(i), at(parent))) return;
      swap(i, parent);
      i = parent;
    }
  };

  const siftDown = (start: number): void => {
    let i = start;
    for (;;) {
      const left = 2 * i + 1;
      const right = left + 1;
      let smallest = i;
      if (left < heap.length && before(at(left), at(smallest))) smallest = left;
      if (right < heap.length && before(at(right), at(smallest))) smallest = right;
      if (smallest === i) return;
      swap(i, smallest);
      i = smallest;
    }
  };

  return {
    push: (time, item) => {
      assertSimTime(time);
      const entry: Scheduled<T> = { time, seq: nextSeq++, item };
      heap.push(entry);
      siftUp(heap.length - 1);
      return entry;
    },
    pop: () => {
      if (heap.length === 0) return undefined;
      const top = at(0);
      const last = heap.pop() as Scheduled<T>;
      if (heap.length > 0) {
        heap[0] = last;
        siftDown(0);
      }
      return top;
    },
    peek: () => heap[0],
    get size() {
      return heap.length;
    },
  };
}
