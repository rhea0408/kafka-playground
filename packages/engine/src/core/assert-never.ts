/** Exhaustiveness guard for switches over discriminated unions. */
export function assertNever(value: never): never {
  throw new Error(`unhandled variant: ${JSON.stringify(value)}`);
}
