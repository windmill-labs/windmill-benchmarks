import { proxyActivities } from '@temporalio/workflow';

const ITER = 40;
const FIBO_N = 10;

const { fiboActivity } = proxyActivities<{
  fiboActivity(n: number): Promise<number>;
}>({
  startToCloseTimeout: '360s', // match Python timedelta(seconds=360)
});

export async function bench10Workflow(): Promise<void> {
  for (let i = 0; i < ITER; i++) {
    await fiboActivity(FIBO_N);
  }
}
