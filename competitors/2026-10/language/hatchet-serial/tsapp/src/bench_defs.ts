/**
 * Shared Hatchet workflow declarations for the orchestration benchmark, TypeScript side.
 *
 * Mirrors app/bench_defs.py one-for-one: two workflows, both strictly sequential,
 * task i+1 declares task i as its parent, each task computes a naive recursive fibo.
 */
import { HatchetClient } from '@hatchet-dev/typescript-sdk';

export const hatchet = HatchetClient.init();

export function fibo(n: number): number {
  return n <= 1 ? n : fibo(n - 1) + fibo(n - 2);
}

function build(name: string, steps: number, n: number) {
  const wf = hatchet.workflow({ name });
  let prev: any = null;
  for (let i = 0; i < steps; i += 1) {
    const make = (nn: number) => () => ({ result: fibo(nn) });
    // annotated `string` on purpose: a template-literal type here makes the SDK's
    // task() overload resolve to the standalone-task arm, which has no `parents`.
    const taskName: string = `s${i}`;
    const t = wf.task({
      name: taskName,
      parents: prev ? [prev] : [],
      fn: make(n),
    });
    prev = t;
  }
  return wf;
}

export const WF_40_10 = build('bench-ts-40-10', 40, 10);
export const WF_10_33 = build('bench-ts-10-33', 10, 33);

export const WORKFLOWS: Record<string, any> = {
  fibonacci_40_10: WF_40_10,
  fibonacci_10_33: WF_10_33,
};
