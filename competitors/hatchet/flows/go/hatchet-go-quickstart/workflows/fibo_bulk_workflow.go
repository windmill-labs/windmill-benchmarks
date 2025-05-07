package workflows

import (
	"fmt"

	"github.com/hatchet-dev/hatchet/pkg/client/create"
	v1 "github.com/hatchet-dev/hatchet/pkg/v1"
	"github.com/hatchet-dev/hatchet/pkg/v1/factory"
	"github.com/hatchet-dev/hatchet/pkg/v1/workflow"
	"github.com/hatchet-dev/hatchet/pkg/worker"
)

type FiboInput struct {
	N int `json:"n"`
}

type FiboTaskOutput struct {
	Result int `json:"result"`
}

func BulkFibonacciWorkflow(hatchet v1.HatchetClient) workflow.WorkflowDeclaration[FiboInput, FiboTaskOutput] {
	wf := factory.NewWorkflow[FiboInput, FiboTaskOutput](
		create.WorkflowCreateOpts[FiboInput]{
			Name: "bulk-fibo-workflow",
		},
		hatchet,
	)

	wf.Task(
		create.WorkflowTask[FiboInput, FiboTaskOutput]{
			Name: "compute_fibo",
		},
		func(ctx worker.HatchetContext, input FiboInput) (any, error) {
			fmt.Printf("Running fibo(%d)\n", input.N)
			return &FiboTaskOutput{Result: fibo(input.N)}, nil
		},
	)

	return wf
}