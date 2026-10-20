import { describe, expect, it } from 'vitest';
import { SCHEMA_VERSION } from './index';

describe('@kp/schema', () => {
  it('exports a version marker', () => {
    expect(SCHEMA_VERSION).toBe('0.0.0');
  });
});
