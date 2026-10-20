/**
 * Scheduled engine work, as plain data so pending work stays inspectable and serialisable
 * (architecture §4.7). Producers and consumers add their tasks in M1-T6 and T7.
 */
export type EngineTask = never;
