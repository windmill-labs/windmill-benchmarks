import { Worker } from '@temporalio/worker';
import { fiboActivity } from './activities';
import { bench10Workflow } from './workflows';
import { Client } from '@temporalio/client';

const ITER = 40;

async function run() {
  const worker = await Worker.create({
    workflowsPath: require.resolve('./workflows'),
    activities: { fiboActivity },
    taskQueue: `bench-${ITER}`,
  });

  const client = new Client();

  // Launch the workflow
  await client.workflow.start(bench10Workflow, {
    workflowId: `bench-${ITER}`,
    taskQueue: `bench-${ITER}`,
  });

  await worker.run();
  process.exit(0);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
