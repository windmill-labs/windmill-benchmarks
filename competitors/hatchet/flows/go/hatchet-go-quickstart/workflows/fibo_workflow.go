package workflows

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/hatchet-dev/hatchet/pkg/client/create"
	v1 "github.com/hatchet-dev/hatchet/pkg/v1"
	"github.com/hatchet-dev/hatchet/pkg/v1/factory"
	"github.com/hatchet-dev/hatchet/pkg/v1/workflow"
	"github.com/hatchet-dev/hatchet/pkg/worker"
)

type FibonacciInput struct {
	N int
}

type FibonacciOutput struct {
	Result int
}

type ParentInput struct {
	N int
}

type ParentOutput struct{}

func FibonacciWorkflow(hatchet v1.HatchetClient) (workflow.WorkflowDeclaration[FibonacciInput, FibonacciOutput], workflow.WorkflowDeclaration[ParentInput, ParentOutput]) {
	fibo := factory.NewTask[FibonacciInput, FibonacciOutput](
		create.StandaloneTask{
			Name: "fibo-task-2",
		},
		func(ctx worker.HatchetContext, input FibonacciInput) (*FibonacciOutput, error) {
			fmt.Println("Fibonacci task called")
			return &FibonacciOutput{
				Result: fibo(input.N),
			}, nil
		},
		hatchet,
	)

	parent := factory.NewDurableTask(
		create.StandaloneTask{
			Name: "fibo-parent-2",
		},
		func(ctx worker.DurableHatchetContext, input ParentInput) (*ParentOutput, error) {
			iterations := 100
			if iterStr := os.Getenv("ITERATIONS"); iterStr != "" {
				if iter, err := strconv.Atoi(iterStr); err == nil && iter > 0 {
					iterations = iter
				}
			}

			parallel := os.Getenv("PARALLEL") != ""

			if parallel {
				wg := sync.WaitGroup{}
				wg.Add(iterations)

				for i := 0; i < iterations; i++ {
					go func() {
						defer wg.Done()
						n := input.N
						key := fmt.Sprintf("fibo-task-%d", i)
						fmt.Printf("Fibonacci task %d called\n", n)
						result, err := fibo.RunAsChild(ctx, FibonacciInput{N: n}, workflow.RunAsChildOpts{
							Key: &key,
						})

						if err != nil {
							fmt.Printf("Error in Fibonacci task %d: %v\n", n, err)
							return
						}

						fmt.Printf("Fibonacci result for %d: %d\n", n, result.Result)
					}()
				}

				wg.Wait()
			} else {
				fmt.Println("Running in sequential mode")

				for i := 0; i < iterations; i++ {
					fmt.Printf("Fibonacci task %d called\n", i)
					n := input.N
					result, err := fibo.RunAsChild(ctx, FibonacciInput{N: n}, workflow.RunAsChildOpts{})

					if err != nil {
						fmt.Printf("Error in Fibonacci task %d: %v\n", n, err)
						return nil, err
					}

					fmt.Printf("Fibonacci result for %d: %d\n", n, result.Result)
				}
			}

			return nil, nil
		},
		hatchet,
	)

	return fibo, parent
}

func fibo(n int) int {
	if n <= 1 {
		return n
	}
	return fibo(n-1) + fibo(n-2)
}
