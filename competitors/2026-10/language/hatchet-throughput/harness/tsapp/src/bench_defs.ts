/**
 * Throughput benchmark task definition, TypeScript side.
 *
 * One standalone task (a single-task workflow in Hatchet terms), so a batch of N
 * runs is N independently runnable tasks: no DAG, no parents, nothing serialising
 * them. The body is a 100ms timer, the same wall-clock cost the Python harness's
 * time.sleep(0.1) body has, so the comparison is engine against engine and not
 * CPython against V8.
 */
import { HatchetClient } from '@hatchet-dev/typescript-sdk';

export const hatchet = HatchetClient.init();

const SLEEP_MS = parseInt(process.env.SLEEP_MS ?? '100', 10);

export const tpSleep = hatchet.task({
  name: 'tp-sleep-ts',
  fn: async (_input: { i: number }) => {
    await new Promise((r) => setTimeout(r, SLEEP_MS));
    return { ok: 1 };
  },
});

export const TASKS: Record<string, any> = { sleep: tpSleep };
