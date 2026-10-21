/**
 * Fixed-capacity FIFO that overwrites its oldest item when full: the host's bounded event history
 * (NFR-2.9: a simulated hour at 10× without memory growth).
 */
export class RingBuffer<T> {
  readonly capacity: number;
  readonly #items: (T | undefined)[];
  #start = 0;
  #size = 0;

  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError(`capacity must be a positive integer, got ${String(capacity)}`);
    }
    this.capacity = capacity;
    this.#items = new Array<T | undefined>(capacity);
  }

  get size(): number {
    return this.#size;
  }

  push(item: T): void {
    const end = (this.#start + this.#size) % this.capacity;
    this.#items[end] = item;
    if (this.#size < this.capacity) this.#size++;
    else this.#start = (this.#start + 1) % this.capacity;
  }

  /** Items oldest first, as a new array. */
  toArray(): T[] {
    const out: T[] = [];
    for (let i = 0; i < this.#size; i++) {
      out.push(this.#items[(this.#start + i) % this.capacity] as T);
    }
    return out;
  }

  clear(): void {
    this.#items.fill(undefined);
    this.#start = 0;
    this.#size = 0;
  }
}
