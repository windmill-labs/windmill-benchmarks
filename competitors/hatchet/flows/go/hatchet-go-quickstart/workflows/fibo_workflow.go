package workflows

import (
	"fmt"
	"os"
	"strconv"

	"github.com/hatchet-dev/hatchet/pkg/client/create"
	v1 "github.com/hatchet-dev/hatchet/pkg/v1"
	"github.com/hatchet-dev/hatchet/pkg/v1/factory"
	"github.com/hatchet-dev/hatchet/pkg/v1/task"
	"github.com/hatchet-dev/hatchet/pkg/v1/workflow"
	"github.com/hatchet-dev/hatchet/pkg/worker"
)

type FibonacciInput struct {
	N int
}

type FibonacciOutput struct {
	Result int
}

type FibonacciResult struct {
	Fibo FibonacciOutput
}

func FibonacciWorkflow(hatchet v1.HatchetClient) workflow.WorkflowDeclaration[FibonacciInput, FibonacciResult] {
	simple := factory.NewWorkflow[FibonacciInput, FibonacciResult](
		create.WorkflowCreateOpts[FibonacciInput]{
			Name: "fibo-workflow",
		},
		hatchet,
	)

	var previous *task.TaskDeclaration[FibonacciInput]
	parallel := os.Getenv("PARALLEL") != ""

	iterations := 100
	if iterStr := os.Getenv("ITERATIONS"); iterStr != "" {
		if iter, err := strconv.Atoi(iterStr); err == nil && iter > 0 {
			iterations = iter
		}
	}

	for i := 0; i < iterations; i++ {
		name := fmt.Sprintf("fibo_%d", i)

		opts := create.WorkflowTask[FibonacciInput, FibonacciResult]{
			Name: name,
		}

		// Only set parents if not running in parallel mode
		if !parallel && previous != nil {
			opts.Parents = []create.NamedTask{previous}
		}

		current := simple.Task(
			opts,
			func(ctx worker.HatchetContext, input FibonacciInput) (any, error) {
				fmt.Println("Fibonacci task called")
				return &FibonacciOutput{
					Result: fibo(input.N),
				}, nil
			},
		)

		previous = current
	}

	return simple
}

func fibo(n int) int {
	if n <= 1 {
		return n
	}
	return fibo(n-1) + fibo(n-2)
}