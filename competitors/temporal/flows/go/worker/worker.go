package worker

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func Fibo(n int) int {
	if n <= 1 {
		return n
	}
	return Fibo(n-1) + Fibo(n-2)
}

func FiboActivity(ctx context.Context, n int) (int, error) {
	return Fibo(n), nil
}

func BenchWorkflow(ctx workflow.Context, iter int, fiboN int, parallel bool) error {
	taskQueue := workflow.GetInfo(ctx).TaskQueueName

	// Sequential execution
	if !parallel {
		for i := 0; i < iter; i++ {
			activityID := fmt.Sprintf("task_%02d", i)
			activityOptions := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				ActivityID:         activityID,
				StartToCloseTimeout: 360 * time.Second,
				TaskQueue:           taskQueue,
			})

			var result int
			if err := workflow.ExecuteActivity(activityOptions, FiboActivity, fiboN).Get(ctx, &result); err != nil {
				return err
			}
		}
		return nil
	}

	// Parallel execution
	futures := make([]workflow.Future, iter)
	for i := 0; i < iter; i++ {
		activityID := fmt.Sprintf("task_%02d", i)
		activityOptions := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:         activityID,
			StartToCloseTimeout: 360 * time.Second,
			TaskQueue:           taskQueue,
		})
		futures[i] = workflow.ExecuteActivity(activityOptions, FiboActivity, fiboN)
	}

	for i := 0; i < iter; i++ {
		var result int
		if err := futures[i].Get(ctx, &result); err != nil {
			return err
		}
	}

	return nil
}

// RunWorker starts a worker process
func RunWorker() {
	// Parse command line arguments
	iter := flag.Int("iter", 10, "Number of iterations")
	fiboN := flag.Int("fibo-n", 33, "Fibonacci number to calculate")
	parallel := flag.Bool("parallel", false, "Run in parallel mode")
	flag.Parse()

	log.Printf("Worker starting with iter=%d, fibo_n=%d, parallel=%v", *iter, *fiboN, *parallel)

	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	taskQueue := fmt.Sprintf("bench-%d-task-queue", *iter)

	workerOptions := worker.Options{
		MaxConcurrentActivityExecutionSize: 1,
	}

	w := worker.New(c, taskQueue, workerOptions)

	w.RegisterWorkflowWithOptions(BenchWorkflow, workflow.RegisterOptions{
		Name: fmt.Sprintf("bench_%d_%d", *iter, *fiboN),
	})
	w.RegisterActivity(FiboActivity)

	// Signal that we're ready by creating a file
	readyFile := fmt.Sprintf("/tmp/worker_%d_%d.ready", *iter, os.Getpid())
	if err := os.WriteFile(readyFile, []byte("ready"), 0644); err != nil {
		log.Fatalf("Failed to create ready file: %v", err)
	}
	defer os.Remove(readyFile)

	log.Printf("Worker ready and registered on task queue: %s", taskQueue)
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}