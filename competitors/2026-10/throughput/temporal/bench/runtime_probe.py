import json
import platform
import sys
import time

import temporalio

from common import fibo

runs = []
for _ in range(3):
    t = time.monotonic()
    v = fibo(31)
    runs.append(round(time.monotonic() - t, 5))
print(
    json.dumps(
        {
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "python_build": " ".join(platform.python_build()),
            "sys_version": sys.version,
            "temporalio_sdk": temporalio.__version__ if hasattr(temporalio, "__version__") else None,
            "fibo31_value": v,
            "fibo31_runs_s": runs,
            "fibo31_mean_s": round(sum(runs) / len(runs), 5),
        },
        indent=1,
    )
)
