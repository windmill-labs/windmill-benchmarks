import argparse
from worker import fibo_wf, WorkflowInput

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type=int, default=10)
    args = parser.parse_args()

    result = fibo_wf.run(WorkflowInput(n=args.n))
    print(result)

if __name__ == "__main__":
    main()