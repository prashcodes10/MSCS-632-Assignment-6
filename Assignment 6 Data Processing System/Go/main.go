package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Task struct {
	ID   int
	Data string
}

func worker(workerID int, tasks <-chan Task, results chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Printf("Worker %d started.\n", workerID)

	for task := range tasks {
		fmt.Printf("Worker %d processing Task %d\n", workerID, task.ID)

		time.Sleep(1 * time.Second)

		result := fmt.Sprintf("Worker %d completed Task %d with data: %s",
			workerID, task.ID, task.Data)

		results <- result
		fmt.Println(result)
	}

	fmt.Printf("Worker %d completed execution.\n", workerID)
}

func writeResultsToFile(results <-chan string, done chan<- bool) {
	file, err := os.Create("go_output.txt")
	if err != nil {
		fmt.Println("File creation error:", err)
		done <- false
		return
	}
	defer file.Close()

	for result := range results {
		_, err := file.WriteString(result + "\n")
		if err != nil {
			fmt.Println("File writing error:", err)
		}
	}

	done <- true
}

func main() {
	numberOfTasks := 10
	numberOfWorkers := 3

	tasks := make(chan Task, numberOfTasks)
	results := make(chan string, numberOfTasks)
	done := make(chan bool)

	var wg sync.WaitGroup

	go writeResultsToFile(results, done)

	for i := 1; i <= numberOfWorkers; i++ {
		wg.Add(1)
		go worker(i, tasks, results, &wg)
	}

	for i := 1; i <= numberOfTasks; i++ {
		tasks <- Task{
			ID:   i,
			Data: fmt.Sprintf("Data item %d", i),
		}
	}

	close(tasks)

	wg.Wait()
	close(results)

	success := <-done
	if success {
		fmt.Println("All Go tasks processed successfully.")
	} else {
		fmt.Println("Go program completed with file errors.")
	}
}
