import inspect
from temporalio.worker import Worker
for k, v in inspect.signature(Worker.__init__).parameters.items():
    print("%-50s %s" % (k, v.default))
