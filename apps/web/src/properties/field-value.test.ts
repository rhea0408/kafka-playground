import { describe, expect, it } from 'vitest';
import {
  checkField,
  describeValue,
  humanDuration,
  parseFieldText,
  splitKeys,
  withUnit,
} from './field-value.ts';
import { defaultStrategy, withPart } from './KeyStrategyField.tsx';

describe('value formatting', () => {
  it('prints durations the way people say them', () => {
    expect(humanDuration(500)).toBe('500 ms');
    expect(humanDuration(1500)).toBe('1.5 s');
    expect(humanDuration(45_000)).toBe('45 s');
    expect(humanDuration(300_000)).toBe('5 min');
    expect(humanDuration(3_600_000)).toBe('1 h');
    expect(humanDuration(604_800_000)).toBe('7 days');
  });

  it('adds units and a friendlier form', () => {
    expect(withUnit(300_000, 'ms')).toBe('300000 ms (5 min)');
    expect(withUnit(500, 'ms')).toBe('500 ms');
    expect(withUnit(16_384, 'bytes')).toBe('16384 bytes (16 KiB)');
    expect(withUnit(1, 'bytes')).toBe('1 byte');
    expect(withUnit(500, 'records')).toBe('500 records');
    expect(withUnit(1, 'replicas')).toBe('1 replica');
    expect(describeValue(null, { type: 'text' })).toBe('none');
    expect(describeValue(true, { type: 'boolean' })).toBe('true');
  });
});

describe('inline validation with the schema', () => {
  it('accepts valid numbers', () => {
    expect(parseFieldText('consumer', 'fetchMaxWaitMs', ' 100 ', { type: 'int' })).toEqual({
      ok: true,
      value: 100,
    });
    expect(parseFieldText('producer', 'ratePerSec', '2.5', { type: 'number' })).toEqual({
      ok: true,
      value: 2.5,
    });
  });

  it('gives the Zod messages from @kp/schema', () => {
    expect(parseFieldText('consumer', 'maxPollRecords', '1.5', { type: 'int' })).toEqual({
      ok: false,
      error: 'max.poll.records must be a whole number.',
    });
    expect(parseFieldText('consumer', 'fetchMaxWaitMs', '-1', { type: 'int' })).toEqual({
      ok: false,
      error: 'fetch.max.wait.ms cannot be negative.',
    });
    expect(parseFieldText('producer', 'ratePerSec', '0', { type: 'number' })).toEqual({
      ok: false,
      error: 'Rate must be greater than 0.',
    });
    expect(parseFieldText('consumer', 'maxPollRecords', 'abc', { type: 'int' }).ok).toBe(false);
    expect(parseFieldText('consumer', 'maxPollRecords', '', { type: 'int' })).toEqual({
      ok: false,
      error: 'Enter a number.',
    });
  });

  it('validates key strategies as a whole', () => {
    expect(checkField('producer', 'keyStrategy', defaultStrategy('skewed')).ok).toBe(true);
    const noKeys = withPart(defaultStrategy('random'), 'keys', ' , ');
    expect(checkField('producer', 'keyStrategy', noKeys)).toEqual({
      ok: false,
      error: 'Random key strategy needs at least one key.',
    });
    const share = withPart(defaultStrategy('skewed'), 'hotShare', '1.5');
    expect(checkField('producer', 'keyStrategy', share)).toEqual({
      ok: false,
      error: 'Hot-key share must be between 0 and 1.',
    });
  });

  it('splits key lists', () => {
    expect(splitKeys('D, E ,F,,')).toEqual(['D', 'E', 'F']);
  });
});
