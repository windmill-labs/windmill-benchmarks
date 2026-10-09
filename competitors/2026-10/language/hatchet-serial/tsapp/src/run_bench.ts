/** Trigger phase: fire the sequential DAG warmup+reps times, emit the run ids. */
import { WORKFLOWS } from './bench_defs';

function arg(name: string, dflt: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : dflt;
}

async function main() {
  const workload = arg('workload', '');
  const warmup = parseInt(arg('warmup', '1'), 10);
  const reps = parseInt(arg('reps', '5'), 10);
  const wf = WORKFLOWS[workload];
  if (!wf) throw new Error(`unknown workload ${workload}`);

  const warmupIds: string[] = [];
  const runIds: string[] = [];

  for (let i = 0; i < warmup + reps; i += 1) {
    const t = Date.now();
    const ref = await wf.runNoWait({});
    const rid = await ref.getWorkflowRunId();
    const res = await ref.output;
    const wall = ((Date.now() - t) / 1000).toFixed(3);
    const label = i < warmup ? 'warmup' : `rep ${i - warmup + 1}`;
    const n = res && typeof res === 'object' ? Object.keys(res).length : 0;
    process.stderr.write(`  ${label}: run ${rid} wall ${wall}s tasks ${n}\n`);
    (i < warmup ? warmupIds : runIds).push(rid);
  }

  process.stdout.write(JSON.stringify({ warmup_ids: warmupIds, run_ids: runIds }) + '\n');
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
