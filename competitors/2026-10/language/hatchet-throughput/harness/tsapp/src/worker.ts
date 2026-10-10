/** One Hatchet worker process. Slot count is explicit (the SDK default is 100). */
import { hatchet, TASKS } from './bench_defs';

function arg(name: string, dflt: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : dflt;
}

async function main() {
  const workload = arg('workload', 'sleep');
  const slots = parseInt(arg('slots', '20'), 10);
  const name = arg('name', 'tp-ts-w1');
  const worker = await hatchet.worker(name, {
    slots,
    workflows: [TASKS[workload]],
  });
  await worker.start();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
