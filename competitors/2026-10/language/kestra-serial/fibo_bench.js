function fibo(n) {
  if (n <= 1) {
    return n
  } else {
    return fibo(n-1) + fibo(n-2)
  }
}
const ts = [];
for (let i = 0; i < 3; i++) {
  const t = process.hrtime.bigint(); fibo(33);
  ts.push(Math.round(Number(process.hrtime.bigint() - t) / 1e6) / 1e3);
}
let t = process.hrtime.bigint();
for (let i = 0; i < 1000; i++) fibo(10);
const f10 = Number(process.hrtime.bigint() - t) / 1e9 / 1000;
console.log(JSON.stringify({version: process.version, fibo33_s: ts, fibo10_s: Number(f10.toPrecision(3))}));
