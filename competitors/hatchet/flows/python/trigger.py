import argparse
from worker import fibonacci_parent, FibonacciTriggerInput

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type=int, default=10)
    args = parser.parse_args()

    result = fibonacci_parent.run(FibonacciTriggerInput(n=args.n))
    print(result)

if __name__ == "__main__":
    main()