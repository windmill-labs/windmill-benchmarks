"""Single Hatchet worker process, default settings (default slot count)."""
from bench_defs import WF_10_33, WF_40_10, hatchet

if __name__ == "__main__":
    worker = hatchet.worker("bench-worker", workflows=[WF_40_10, WF_10_33])
    worker.start()
