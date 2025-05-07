# Hatchet Fibonacci Workflow

This is a Hatchet workflow implementation that computes Fibonacci numbers with configurable iterations and workers.

## Prerequisites

1. Python 3.8+
2. Hatchet server running locally
3. Hatchet CLI installed

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Workflow

1. Start the Hatchet server (if not already running):
```bash
hatchet server start
```

2. Run the workflow:
```bash
python fibonacci.py
```

## Configuration

The workflow accepts the following parameters:
- `iterations`: Number of times to compute Fibonacci (default: 10)
- `n`: The Fibonacci number to compute (default: 33)
- `workers`: Number of concurrent workers (default: 1)

Example configurations:
- Sequential execution (1 worker):
```python
input_data = {
    "iterations": 10,
    "n": 33,
    "workers": 1
}
```

- Parallel execution (multiple workers):
```python
input_data = {
    "iterations": 10,
    "n": 33,
    "workers": 4
}
```