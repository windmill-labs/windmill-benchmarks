"""One Hatchet worker process. Slot count is explicit (Hatchet's default is 100)."""
import argparse

from bench_defs import TASKS, hatchet


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", required=True, choices=sorted(TASKS))
    ap.add_argument("--slots", type=int, required=True)
    ap.add_argument("--name", required=True)
    a = ap.parse_args()
    hatchet.worker(a.name, slots=a.slots, workflows=[TASKS[a.workload]]).start()


if __name__ == "__main__":
    main()
