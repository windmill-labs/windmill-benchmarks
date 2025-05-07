package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.temporal.io/sdk/client"
)

func main() {
	// Parse command line arguments
	iter := flag.Int("iter", 10, "Number of iterations")
	fiboN := flag.Int("fibo-n", 33, "Fibonacci number to calculate")
	workers := flag.Int("workers", 1, "Number of worker processes")
	parallel := flag.Bool("parallel", false, "Run in parallel mode")
	flag.Parse()

	log.Printf("Starting benchmark with %d workers, %d iterations, fibo(%d), parallel=%v", *workers, *iter, *fiboN, *parallel)

	// Keep track of worker processes
	var workerProcesses []*os.Process
	var wg sync.WaitGroup

	// Start worker processes
	for i := 0; i < *workers; i++ {
		cmd := exec.Command("go", "run", "./cmd/worker/main.go",
			"-iter", strconv.Itoa(*iter),
			"-fibo-n", strconv.Itoa(*fiboN),
			fmt.Sprintf("-parallel=%v", *parallel))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		log.Printf("Starting worker %d with command: %v", i, cmd.Args)
		if err := cmd.Start(); err != nil {
			log.Fatalf("Failed to start worker %d: %v", i, err)
		}
		workerProcesses = append(workerProcesses, cmd.Process)
		log.Printf("Started worker %d with PID %d", i, cmd.Process.Pid)

		// Set up wait group for worker
		wg.Add(1)
		go func(cmd *exec.Cmd) {
			defer wg.Done()
			cmd.Wait()
		}(cmd)
	}

	// Wait for all workers to signal ready
	log.Printf("Waiting for %d workers to initialize...", *workers)
	readyCount := 0
	deadline := time.Now().Add(10 * time.Second)
	for readyCount < *workers && time.Now().Before(deadline) {
		files, err := filepath.Glob(fmt.Sprintf("/tmp/worker_%d_*.ready", *iter))
		if err != nil {
			log.Fatalf("Failed to check for ready files: %v", err)
		}
		if len(files) > readyCount {
			readyCount = len(files)
			log.Printf("%d/%d workers ready...", readyCount, *workers)
		}
		if readyCount < *workers {
			time.Sleep(100 * time.Millisecond)
		}
	}

	if readyCount < *workers {
		log.Fatalf("Timed out waiting for workers to initialize. Only %d/%d workers ready", readyCount, *workers)
	}

	log.Printf("All %d workers ready! Starting workflow...", *workers)

	// Create a client to talk to the Temporal server
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	// Define workflow options
	workflowID := fmt.Sprintf("bench_%d_%d", *iter, *fiboN)
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: fmt.Sprintf("bench-%d-task-queue", *iter),
	}

	log.Printf("Starting workflow with ID %s on task queue %s", workflowOptions.ID, workflowOptions.TaskQueue)

	// Start the workflow with parameters
	we, err := c.ExecuteWorkflow(
		context.Background(),
		workflowOptions,
		workflowID,
		*iter,
		*fiboN,
		*parallel,
	)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}

	log.Printf("Started workflow: WorkflowID: %s, RunID: %s\n", we.GetID(), we.GetRunID())

	// Wait for workflow completion
	var result interface{}
	err = we.Get(context.Background(), &result)
	if err != nil {
		log.Fatalln("Unable to get workflow result", err)
	}

	log.Println("Workflow completed! Cleaning up workers...")

	// Kill all worker processes
	for _, process := range workerProcesses {
		if err := process.Kill(); err != nil {
			log.Printf("Error killing worker process %d: %v", process.Pid, err)
		}
	}

	// Wait for all processes to be cleaned up
	wg.Wait()
	log.Println("All workers cleaned up. Benchmark complete!")
}