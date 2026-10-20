import { describe, expect, it } from 'vitest';
import { ENGINE_VERSION } from './index';

describe('@kp/engine', () => {
  it('exports a version marker', () => {
    expect(ENGINE_VERSION).toBe('0.0.0');
  });
});
