export function fiboActivity(n: number): number {
  if (n <= 1) {
    return n;
  }
  return fiboActivity(n - 1) + fiboActivity(n - 2);
}

