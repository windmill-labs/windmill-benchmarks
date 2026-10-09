"""fibo(31) timed in both bodies: a @task thread in the flow process, and a
deployment run executed as its own subprocess by the process worker."""
import json
import platform
import sys
import time

from prefect import flow, task


def fibo(n: int) -> int:
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


def three():
    fibo(20)
    out = []
    for _ in range(3):
        s = time.perf_counter()
        fibo(31)
        out.append(round(time.perf_counter() - s, 4))
    return out


@task(name="probe_task", persist_result=False, cache_policy=None)
def probe_task():
    import prefect
    return {"where": "in_process @task thread", "pid": __import__("os").getpid(),
            "python": platform.python_version(), "full": sys.version,
            "prefect": prefect.__version__, "fibo31_s": three()}


@flow(name="probe_inproc", persist_result=False)
def probe_inproc():
    return probe_task()


@flow(name="probe_pool", persist_result=False)
def probe_pool():
    import prefect
    r = {"where": "work_pool deployment subprocess", "pid": __import__("os").getpid(),
         "python": platform.python_version(), "full": sys.version,
         "prefect": prefect.__version__, "fibo31_s": three()}
    print("PROBE_POOL " + json.dumps(r), flush=True)
    return r


if __name__ == "__main__":
    if sys.argv[1] == "inproc":
        print("PROBE_INPROC " + json.dumps(probe_inproc()), flush=True)
    elif sys.argv[1] == "deploy":
        flow.from_source(source="/opt/flows", entrypoint="probe_both.py:probe_pool").deploy(
            name="probe", work_pool_name="bench")
        print("deployed probe")
    elif sys.argv[1] == "runpool":
        from prefect.deployments import run_deployment
        fr = run_deployment(name="probe_pool/probe", timeout=None, poll_interval=1)
        print("PROBE_POOL_STATE " + str(fr.state_type) + " " + str(fr.id), flush=True)
