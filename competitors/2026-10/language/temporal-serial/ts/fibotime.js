function fibo(n) {
  if (n <= 1) {
    return n;
  }
  return fibo(n - 1) + fibo(n - 2);
}

const runs = [];
for (let i = 0; i < 3; i++) {
  const t = process.hrtime.bigint();
  fibo(33);
  runs.push(Math.round(Number(process.hrtime.bigint() - t) / 1e5) / 1e4);
}
console.log(
  JSON.stringify({
    version: process.versions.node,
    v8: process.versions.v8,
    fibo33_runs_s: runs,
    fibo33_total_s: Math.round(runs.reduce((a, b) => a + b, 0) * 1e4) / 1e4,
  })
);
