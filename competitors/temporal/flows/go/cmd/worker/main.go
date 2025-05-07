package main

import (
	"log"

	"temporal-bench/worker"
)

func main() {
	log.Println("Starting worker process...")
	worker.RunWorker()
}