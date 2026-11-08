/**
 * Number of fast-check cases for a property file, from an environment variable (test-only code).
 * PR CI runs the default under coverage; the nightly job (.github/workflows/nightly.yml) sets the
 * variable to 1000 for a deep search. The engine has no Node types, so the environment is read
 * through globalThis.
 */
export function runsFromEnv(name: string, fallback: number): number {
  const value = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process
    ?.env?.[name];
  if (value === undefined || value === '') return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < 1) throw new Error(`${name} must be a positive integer`);
  return n;
}
