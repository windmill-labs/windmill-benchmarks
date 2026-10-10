/** Single Hatchet worker process, default settings (default slot count). */
import { hatchet, WF_40_10, WF_10_33 } from './bench_defs';

async function main() {
  const worker = await hatchet.worker('bench-worker-ts', {
    workflows: [WF_40_10, WF_10_33],
  });
  await worker.start();
}

main();
