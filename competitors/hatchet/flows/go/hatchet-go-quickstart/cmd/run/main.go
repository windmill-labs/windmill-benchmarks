package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	hatchet_client "hatchet-go-quickstart/hatchet_client"
	workflows "hatchet-go-quickstart/workflows"
)

func main() {
	n := flag.Int("n", 10, "Fibonacci number to compute")
	iterations := flag.Int("iterations", 1, "Number of bulk iterations")
	bulk := flag.Bool("bulk", false, "Run multiple independent Fibonacci workflows")
	flag.Parse()

	hatchet, err := hatchet_client.HatchetClient()
	if err != nil {
		log.Fatalf("failed to initialize hatchet client: %v", err)
	}

	if *bulk {
		fmt.Printf("Running %d independent Fibonacci workflows with n=%d...\n", *iterations, *n)

		wf := workflows.BulkFibonacciWorkflow(hatchet)

		var inputs []workflows.FiboInput
		for i := 0; i < *iterations; i++ {
			inputs = append(inputs, workflows.FiboInput{
				N: *n,
			})
		}

		runIDs, err := wf.RunBulkNoWait(context.Background(), inputs)
		if err != nil {
			log.Fatalf("RunBulkNoWait failed: %v", err)
		}

		for _, id := range runIDs {
			fmt.Println("Started workflow run:", id)
		}
	} else {
		fmt.Printf("Running single Fibonaccii workflow for n=%d...\n", *n)

		wf := workflows.FibonacciWorkflow(hatchet)

		result, err := wf.Run(context.Background(), workflows.FibonacciInput{
			N: *n,
		})

		if err != nil {
			log.Fatalf("workflow run failed: %v", err)
		}

		fmt.Printf("fibo(%d) = %d\n", *n, result.Fibo.Result)
	}
}