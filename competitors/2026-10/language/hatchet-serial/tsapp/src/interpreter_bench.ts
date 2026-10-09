/**
 * Raw Node speed in the exact image the Hatchet TypeScript worker tasks run in.
 * Standalone on purpose (no hatchet import) so it can be exec'd inside the live
 * worker container. The recursion is byte-for-byte the one the benchmark tasks use.
 */
function fibo(n: number): number {
  return n <= 1 ? n : fibo(n - 1) + fibo(n - 2);
}

function timed(n: number): number {
  const t = process.hrtime.bigint();
  fibo(n);
  return Number(process.hrtime.bigint() - t) / 1e9;
}

const r = (x: number) => Math.round(x * 1e6) / 1e6;

fibo(20); // warm the code path so we are not timing first-call overhead
process.stdout.write(
  JSON.stringify({
    version: `node ${process.version} v8 ${process.versions.v8}`,
    fibo33_s: [r(timed(33)), r(timed(33)), r(timed(33))],
    fibo10_s: r(timed(10)),
  }) + '\n'
);
