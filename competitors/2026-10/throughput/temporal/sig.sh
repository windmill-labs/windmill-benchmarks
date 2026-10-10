set -euo pipefail
docker run --rm bench-py python - <<'PY'
import inspect
from temporalio.worker import Worker
sig = inspect.signature(Worker.__init__)
for k,v in sig.parameters.items():
    if any(s in k for s in ("poll","concurrent","slot","activit","tuner","rate","limit")):
        print(k, "=", v.default)
PY
