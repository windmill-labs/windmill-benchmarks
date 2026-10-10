/**
 * One repetition of the throughput benchmark, TypeScript client.
 *
 * Pre-enqueue is real, not approximated: every connected worker is paused before
 * the bulk trigger and unpaused immediately after it returns, so all N tasks are
 * sitting in Hatchet's queue before any of them is assigned to a slot.
 *
 * Prints a JSON line with the enqueue wall time and the paused window.
 */
import { TASKS } from './bench_defs';
import { listWorkers, setPaused } from './hatchet_api';

function arg(name: string, dflt: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : dflt;
}

async function main() {
  const workload = arg('workload', 'sleep');
  const n = parseInt(arg('n', '2000'), 10);
  const tag = arg('tag', '');
  const expect = parseInt(arg('expect-workers', '1'), 10);
  const noPause = process.argv.includes('--no-pause');
  if (!tag) throw new Error('--tag is required');
  const task = TASKS[workload];
  if (!task) throw new Error(`unknown workload ${workload}`);

  const workers = await listWorkers();
  if (workers.length !== expect) {
    process.stderr.write(`FATAL: ${workers.length} active workers, expected ${expect}\n`);
    process.exit(2);
  }
  const ids = workers.map((w: any) => w.metadata.id);

  // Build every trigger config up front; this is pure client-side work.
  const items = Array.from({ length: n }, (_, i) => ({
    input: { i },
    opts: { additionalMetadata: { rep: tag } },
  }));

  let pausedAt: number | null = null;
  if (!noPause) {
    await setPaused(ids, true);
    pausedAt = Date.now();
    await new Promise((r) => setTimeout(r, 500)); // let the assignment loop see the pause
  }

  const t0 = Date.now();
  const refs = await task.runManyNoWait(items);
  const t1 = Date.now();

  if (!noPause) await setPaused(ids, false);
  const t2 = Date.now();

  process.stdout.write(
    JSON.stringify({
      tag,
      workload,
      n: refs.length,
      enqueue_s: Number(((t1 - t0) / 1000).toFixed(4)),
      paused_window_s: pausedAt ? Number(((t2 - pausedAt) / 1000).toFixed(4)) : null,
      unpause_s: Number(((t2 - t1) / 1000).toFixed(4)),
      workers: ids.length,
    }) + '\n'
  );
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
