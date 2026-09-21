package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	hatchet_client "hatchet-go-quickstart/hatchet_client"
	workflows "hatchet-go-quickstart/workflows"
)

func main() {
	n := flag.Int("n", 10, "Fibonacci number to compute")
	flag.Parse()

	hatchet, err := hatchet_client.HatchetClient()
	if err != nil {
		log.Fatalf("failed to initialize hatchet client: %v", err)
	}

	fmt.Printf("Running single Fibonaccii workflow for n=%d...\n", *n)

	_, parent := workflows.FibonacciWorkflow(hatchet)

	start := time.Now()

	_, err = parent.Run(context.Background(), workflows.ParentInput{
		N: *n,
	})

	if err != nil {
		log.Fatalf("workflow run failed: %v", err)
	}

	fmt.Printf("fibo(%d) workflow completed successfully in %v\n", *n, time.Since(start))
}
