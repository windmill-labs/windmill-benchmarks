import sys, time, json
sys.setrecursionlimit(10000)
def fibo(n):
    if n <= 1:
        return n
    else:
        return fibo(n - 1) + fibo(n - 2)
ts = []
for _ in range(3):
    t = time.perf_counter(); fibo(33); ts.append(round(time.perf_counter() - t, 3))
t = time.perf_counter()
for _ in range(1000): fibo(10)
f10 = (time.perf_counter() - t) / 1000
print(json.dumps({"version": sys.version.split("\n")[0], "fibo33_s": ts, "fibo10_s": float("%.9g" % f10)}))
