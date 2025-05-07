import { proxyActivities } from '@temporalio/workflow';

const ITER = 10;
const FIBO_N = 33;

const { fiboActivity } = proxyActivities<{
  fiboActivity(n: number): Promise<number>;
}>({
  startToCloseTimeout: '360s',
});

export async function bench10Workflow(): Promise<void> {
  for (let i = 0; i < ITER; i++) {
    await fiboActivity(FIBO_N);
  }
}
