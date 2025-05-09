package main

import (
	hatchet_client "hatchet-go-quickstart/hatchet_client"
	workflows "hatchet-go-quickstart/workflows"

	"github.com/hatchet-dev/hatchet/pkg/cmdutils"
	"github.com/hatchet-dev/hatchet/pkg/v1/worker"
	"github.com/hatchet-dev/hatchet/pkg/v1/workflow"
)

func main() {

	hatchet, err := hatchet_client.HatchetClient()

	if err != nil {
		panic(err)
	}

	worker, err := hatchet.Worker(
		worker.WorkerOpts{
			Name: "fibo-workflow-worker",
			Workflows: []workflow.WorkflowBase{
				workflows.FibonacciWorkflow(hatchet),
				workflows.BulkFibonacciWorkflow(hatchet),
			},
			Slots: 1,
		},
	)

	if err != nil {
		panic(err)
	}

	interruptCtx, cancel := cmdutils.NewInterruptContext()

	defer cancel()

	err = worker.StartBlocking(interruptCtx)

	if err != nil {
		panic(err)
	}
}
