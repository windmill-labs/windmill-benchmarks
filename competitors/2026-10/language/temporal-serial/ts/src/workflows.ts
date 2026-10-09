import { proxyActivities } from '@temporalio/workflow';

const { fiboActivity } = proxyActivities<{
  fiboActivity(n: number): Promise<number>;
}>({
  startToCloseTimeout: '360s',
});

export async function benchWorkflow(iter: number, fiboN: number): Promise<void> {
  for (let i = 0; i < iter; i++) {
    await fiboActivity(fiboN);
  }
}
