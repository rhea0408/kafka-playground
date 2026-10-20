/**
 * Turns Zod issues into one readable line each, naming the component by its id where possible:
 * `topic "orders" → partitions: Partitions must be at least 1.` instead of `topics.0.partitions`.
 */

import type { z } from 'zod';

const LIST_LABELS: Record<string, string> = {
  brokers: 'broker',
  topics: 'topic',
  producers: 'producer',
  consumers: 'consumer',
  connections: 'connection',
};

export function formatIssues(issues: readonly z.core.$ZodIssue[], input: unknown): string[] {
  return issues.map((issue) => {
    const where = describePath(issue.path, input);
    return where === '' ? issue.message : `${where}: ${issue.message}`;
  });
}

function describePath(path: readonly PropertyKey[], input: unknown): string {
  const [first, second, ...rest] = path;
  if (typeof first === 'string' && typeof second === 'number' && first in LIST_LABELS) {
    const label = LIST_LABELS[first] ?? first;
    const item = itemAt(input, first, second);
    const name =
      first === 'connections'
        ? describeConnection(item, second)
        : `${label} ${idOf(item) ?? `#${String(second + 1)}`}`;
    return [name, ...rest.map(String)].join(' → ');
  }
  return path.map(String).join('.');
}

function itemAt(input: unknown, key: string, index: number): unknown {
  if (typeof input !== 'object' || input === null) return undefined;
  const list: unknown = (input as Record<string, unknown>)[key];
  return Array.isArray(list) ? (list[index] as unknown) : undefined;
}

function idOf(item: unknown): string | undefined {
  if (typeof item !== 'object' || item === null || !('id' in item)) return undefined;
  return typeof item.id === 'string' && item.id !== '' ? `"${item.id}"` : undefined;
}

function describeConnection(item: unknown, index: number): string {
  if (typeof item === 'object' && item !== null && 'from' in item && 'to' in item) {
    return `connection ${String(item.from)} → ${String(item.to)}`;
  }
  return `connection #${String(index + 1)}`;
}
