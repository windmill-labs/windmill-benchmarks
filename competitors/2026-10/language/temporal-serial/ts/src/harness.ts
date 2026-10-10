import { NativeConnection, Worker } from '@temporalio/worker';
import { Client, Connection } from '@temporalio/client';
import * as activities from './activities';

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

async function main() {
  const iter = Number(process.argv[2]);
  const fiboN = Number(process.argv[3]);
  const reps = Number(process.argv[4]);
  const gap = Number(process.argv[5]);
  const tag = process.argv[6];
  const address = process.env.TEMPORAL_ADDRESS || 'temporal:7233';
  const taskQueue = `bench-${tag}`;

  const nativeConnection = await NativeConnection.connect({ address });
  const worker = await Worker.create({
    connection: nativeConnection,
    namespace: 'default',
    workflowsPath: require.resolve('./workflows'),
    activities,
    taskQueue,
  });
  const runPromise = worker.run();

  const connection = await Connection.connect({ address });
  const client = new Client({ connection, namespace: 'default' });

  // give the pollers a moment to register on the task queue
  await sleep(2000);

  for (let r = 0; r < reps; r++) {
    const t0 = process.hrtime.bigint();
    await client.workflow.execute('benchWorkflow', {
      workflowId: `${tag}-rep${r}`,
      taskQueue,
      args: [iter, fiboN],
    });
    const wall = Number(process.hrtime.bigint() - t0) / 1e9;
    console.log(`rep ${r} wall ${wall.toFixed(3)}`);
    if (r < reps - 1) {
      await sleep(gap * 1000);
    }
  }

  worker.shutdown();
  await runPromise;
  await connection.close();
  await nativeConnection.close();
  console.log('DONE');
}

main().then(
  () => process.exit(0),
  (err) => {
    console.error(err);
    process.exit(1);
  }
);
