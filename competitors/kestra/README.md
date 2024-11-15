# Kestra
## Setup
Setup via [docker compose](./docker-compose.yml) according to the Kestra [documentation](https://kestra.io/docs/installation/docker-compose).

## Create a flow
Usine the UI exposed on [http://localhost:8080](http://https://localhost:8080), create a new flow called "bechmark" based on this [flow in the flows folder](./flows/bench.yml). This flow will have two inputs, `n` for the Fibonacci number to compute and `iters` for the number of iterations. Note that we're adding a concurrency limit of 1 in order to make those tasks sequentially like for other workflow engine benchmarks.

## Execute the flow
To execute, you can use Kestra's API's with `curl` and `jq` as below:

```
# n=10 iters=40
curl -v -X POST -H 'Content-Type: multipart/form-data' -F 'n=10' -F 'iters=40' 'http://localhost:8080/api/v1/executions/company.team/benchmark | jq'

# n=33 iters=10
curl -s -X POST -H 'Content-Type: multipart/form-data' -F 'n=33' -F 'iters=10' 'http://localhost:8080/api/v1/executions/company.team/benchmark | jq'
```

once submitted, you'll get a json response:

```json
{
  "id": "3qxh3cISZgXWSEf0OQqNLA",
  "namespace": "company.team",
  "flowId": "benchmark",
  "flowRevision": 17,
  "inputs": {
    "iters": 10,
    "n": 33
  },
  "state": {
    "current": "CREATED",
    "histories": [
      {
        "state": "CREATED",
        "date": "2024-11-15T18:46:07.746708441Z"
      }
    ],
    "startDate": "2024-11-15T18:46:07.746708441Z",
    "duration": "PT0.00553339S"
  },
  "originalId": "3qxh3cISZgXWSEf0OQqNLA",
  "deleted": false,
  "metadata": {
    "attemptNumber": 1,
    "originalCreatedDate": "2024-11-15T18:46:07.746710930Z"
  },
  "url": "http://localhost:8080//ui/executions/company.team/myflow/3qxh3cISZgXWSEf0OQqNLA"
}
```

## Parse results with timings
to break down the benchmark timings, we again use the Kestra API passing the `execution.id` from above and the [analysis script](./analysis.py)

### n=33 iters=10
```
# n=33 iters=10
ubuntu@ip-172-31-40-232:~/windmill-benchmarks/competitors/kestra$ python3 analysis.py 2EyuS1Tr02Tx7pfpZpU1ut
                  Task ID Value                     Created Time  ... Assignment Time (s)  Execution Time (s)  Total Time (s)
0  7NmK0CY3fA3ZXNCz9wmOOM     0 2024-11-15 19:39:58.722000+00:00  ...               0.047               1.301           1.348
1  7Q0UMcV3Ix30fxAtl2afnZ     1 2024-11-15 19:40:00.119000+00:00  ...               0.054               1.281           1.384
2  4qADuFFah5phmSzjBftWzK     2 2024-11-15 19:40:01.486000+00:00  ...               0.047               1.320           1.399
3   O1NKJkmmgJQLJlV28RZ5P     3 2024-11-15 19:40:02.933000+00:00  ...               0.065               1.309           1.454
4  4FmLy2z94E577wkumuGOId     4 2024-11-15 19:40:04.363000+00:00  ...               0.047               1.364           1.467
5  1HPCvnVjVQlHQ9KhdOewYk     5 2024-11-15 19:40:05.819000+00:00  ...               0.060               1.313           1.418
6  1EisaMjZfjtjKir9rBSkJy     6 2024-11-15 19:40:07.242000+00:00  ...               0.046               1.366           1.462
7   h8dg1avXelN3T3MkhOgLR     7 2024-11-15 19:40:08.713000+00:00  ...               0.058               1.308           1.425
8  7SLTJgEEsa4Nc5efhDKgOO     8 2024-11-15 19:40:10.133000+00:00  ...               0.044               1.337           1.435
9  26IP2m3OcaE3LCQ4PckHoW     9 2024-11-15 19:40:11.542000+00:00  ...               0.048               1.287           1.363

[10 rows x 8 columns]

Percentage Breakdown:
Execution (%): 93.15%
Assignment (%): 3.65%
Transition (%): 3.20%

Total Running Time: 14.15 seconds
```

### n=10 iters=40
```
ubuntu@ip-172-31-40-232:~/windmill-benchmarks/competitors/kestra$ python3 analysis.py 3ZJwJ7BrJqhYN7jlMbcBjU
                   Task ID Value                     Created Time  ... Assignment Time (s)  Execution Time (s)  Total Time (s)
0   7IXCxkIId9x0Bx2WJv3gh4     0 2024-11-15 19:41:26.605000+00:00  ...               0.062               0.770           0.832
1   4pBOuf9hQeNSQinLK0N5eb     1 2024-11-15 19:41:27.462000+00:00  ...               0.056               0.591           0.672
2   63Pd8LObXgfaKMg1NcZ9IL     2 2024-11-15 19:41:28.141000+00:00  ...               0.045               0.613           0.690
3   2aqfYlqh46hfe4wMSKzH6I     3 2024-11-15 19:41:28.873000+00:00  ...               0.058               0.645           0.777
4   583PsNEiU5ZgeUuhcfOK6E     4 2024-11-15 19:41:29.630000+00:00  ...               0.045               0.617           0.716
5   1nXfdBXzwXndsHWa27Jpza     5 2024-11-15 19:41:30.333000+00:00  ...               0.059               0.616           0.716
6   6M91MLoVnSJi5gmSEg1LoV     6 2024-11-15 19:41:31.044000+00:00  ...               0.045               0.624           0.705
7    kAPyTR5JkWNo3yyuf1mky     7 2024-11-15 19:41:31.752000+00:00  ...               0.056               0.602           0.697
8    tqMvGfZB3UCFigfuZOHJf     8 2024-11-15 19:41:32.457000+00:00  ...               0.048               0.691           0.786
9    wFKxs0hrblTcv2WxIG8Go     9 2024-11-15 19:41:33.244000+00:00  ...               0.082               0.610           0.740
10  25fanYrFhrFFcDvAS7CJyG    10 2024-11-15 19:41:34.005000+00:00  ...               0.070               0.607           0.746
11  6Y7r0HqhW4EecB1XxrLVuB    11 2024-11-15 19:41:34.713000+00:00  ...               0.056               0.610           0.697
12  6SUDarAfdQxruOzp8JMhIK    12 2024-11-15 19:41:35.422000+00:00  ...               0.045               0.637           0.725
13  1qemXADXO2HNXbmI0MyqIL    13 2024-11-15 19:41:36.138000+00:00  ...               0.051               0.618           0.703
14  6ncr4VkaINynKUGQVOXMeV    14 2024-11-15 19:41:36.848000+00:00  ...               0.061               0.660           0.762
15  2yU3IoKHlQ6hJ5G6ltIbxX    15 2024-11-15 19:41:37.610000+00:00  ...               0.095               0.670           0.806
16   J8xl0b7ou2aGJ6OanMa5z    16 2024-11-15 19:41:38.417000+00:00  ...               0.068               0.684           0.794
17  7Q1eFvTcMyTazcEssmMwKu    17 2024-11-15 19:41:39.204000+00:00  ...               0.056               0.652           0.743
18  1jnyX4qjn8q41bxiELS3mP    18 2024-11-15 19:41:39.968000+00:00  ...               0.063               0.638           0.757
19  46dtb5K9OLaXWVcJNTUkLy    19 2024-11-15 19:41:40.710000+00:00  ...               0.071               0.645           0.757
20  7B2NDN550AMRQIGrVxIObf    20 2024-11-15 19:41:41.463000+00:00  ...               0.070               0.675           0.782
21   TDkgGMmPhCuC55XmdWiUJ    21 2024-11-15 19:41:42.315000+00:00  ...               0.071               0.638           0.816
22  32aI6g5Kn8b5f61P4M2CJo    22 2024-11-15 19:41:43.065000+00:00  ...               0.065               0.634           0.740
23  7Bwj0M6PPfvJRHsPeXgeI7    23 2024-11-15 19:41:43.808000+00:00  ...               0.067               0.683           0.794
24   sQSRXy46hRjYqEfeJhRdR    24 2024-11-15 19:41:44.610000+00:00  ...               0.059               0.622           0.733
25   JM5Rf2bUKPlqf9avv3Bj2    25 2024-11-15 19:41:45.334000+00:00  ...               0.055               0.612           0.710
26  3jKS4XFkviV1cmxPZ9uDlS    26 2024-11-15 19:41:46.051000+00:00  ...               0.057               0.603           0.710
27   uTM6wQt3e6w2DIMAJb17q    27 2024-11-15 19:41:46.749000+00:00  ...               0.051               0.626           0.715
28  5riRvf8aULQKZzHSxyvfy6    28 2024-11-15 19:41:47.479000+00:00  ...               0.068               0.624           0.745
29   iRe4OqHsrpDBqN0oyRIH5    29 2024-11-15 19:41:48.255000+00:00  ...               0.061               0.633           0.778
30  2lcjRetsYL1IAcubjFkFKg    30 2024-11-15 19:41:49.001000+00:00  ...               0.065               0.612           0.729
31  7cvVDYjsUpPcLiF8DIGqXe    31 2024-11-15 19:41:49.722000+00:00  ...               0.069               0.634           0.747
32  6ElOFJ8ZBb46MopwaKtIVA    32 2024-11-15 19:41:50.475000+00:00  ...               0.061               0.637           0.748
33  4fOFWgKvUoiaLokIkssd8c    33 2024-11-15 19:41:51.227000+00:00  ...               0.078               0.615           0.747
34   afva3fLga1tUq0ReFREe9    34 2024-11-15 19:41:51.960000+00:00  ...               0.070               0.615           0.725
35  47SFhGfMgLIsBOlWD1vGj3    35 2024-11-15 19:41:52.689000+00:00  ...               0.061               0.667           0.772
36  72Z2kxexfAGpCzZTA8IgAH    36 2024-11-15 19:41:53.482000+00:00  ...               0.062               0.622           0.749
37  6ucKaZKxdNgzRln0TMefhR    37 2024-11-15 19:41:54.208000+00:00  ...               0.058               0.616           0.716
38  63IpIC1qDG7Rdj8Kbpgd7W    38 2024-11-15 19:41:54.935000+00:00  ...               0.072               0.635           0.760
39  5eb7K9xE7a5WbQiGK7uZO0    39 2024-11-15 19:41:55.720000+00:00  ...               0.057               0.630           0.765

[40 rows x 8 columns]

Percentage Breakdown:
Execution (%): 85.34%
Assignment (%): 8.28%
Transition (%): 6.38%

Total Running Time: 29.80 seconds
```
