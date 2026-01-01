package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"
)

// Generic Stages
type StreamURL struct {
	Filepath string
}

type DownloadURL struct {
	NumWorkers int
}

type WritePlainText struct {
	Filepath string
}

type WriteQA struct {
    Filepath string
}

type AnalyzeDataset struct {
    Filepath   string
    PythonPath string // Usually "python" or "python3"
}

// Task represents a unit of work in the pipeline
type Task struct {
	ID      int
	URL     string
	Source  string
	Content string
}

type Pipeline interface {
	Stage(context.Context, chan Task) chan Task
}

func RunPipeline(ctx context.Context, stages ...Pipeline) chan Task {
    var in chan Task
    for _, stage := range stages {
        in = stage.Stage(ctx, in)
    }
    return in
}

func (s *StreamURL) Stage(ctx context.Context, in chan Task) chan Task {
	out := make(chan Task)
	go func() {
		defer close(out)
		file, err := os.Open(s.Filepath)
		if err != nil {
			log.Println("Error opening file=", s.Filepath, " with error=", err)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		// scanner.Scan() // Read header
		id := 0

		for scanner.Scan() {
			select {
			case <-ctx.Done():
				log.Println("Stopping CSV reading due to ctx cancelled")
				return
			case out <- Task{ID: id, URL: scanner.Text()}:
				id++
			}
		}

		if err := scanner.Err(); err != nil {
			log.Println("Error reading CSV file=", s.Filepath, " with error=", err)
		}
		log.Println("Finished reading CSV file=", s.Filepath)
	}()
	return out
}

func (d *DownloadURL) Stage(ctx context.Context, in chan Task) chan Task {
	out := make(chan Task)

	client := &http.Client{
		Timeout: 15 * time.Second,
	}

	go func() {
		defer close(out)

		// Semaphore pattern
		sem := make(chan struct{}, d.NumWorkers)

		var wg sync.WaitGroup

		for task := range in {
			// Check context before starting new work
			select {
			case <-ctx.Done():
				return
			default:
			}

			wg.Add(1)
			sem <- struct{}{}

			go func(t Task) {
				defer wg.Done()
				defer func() { <-sem }()

				// --- Download Logic ---
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
				if err != nil {
					log.Println("NewRequest failed:", err)
					return
				}

				req.Header.Set("User-Agent", "LLM-Data-Pipeline/1.0")
				req.Header.Set("Connection", "close")

				resp, err := client.Do(req)
				if err != nil {
					log.Printf("Failed: %s | %v\n", t.URL, err)
					return
				}
				defer resp.Body.Close()

				content, err := io.ReadAll(resp.Body)
				if err != nil {
					log.Printf("Read failed: %s | %v\n", t.URL, err)
					return
				}

				// Send to output
				select {
				case <-ctx.Done():
					return
				case out <- Task{ID: t.ID, URL: t.URL, Content: string(content)}:
				}
			}(task)
		}

		// Wait for all downloads to finish before closing the channel
		wg.Wait()
	}()
	return out
}

func (w *WritePlainText) Stage(ctx context.Context, in chan Task) chan Task {
    out := make(chan Task)
    go func() {
        defer close(out)
        file, err := os.Create(w.Filepath)
        if err != nil {
            log.Println("Error creating file=", w.Filepath, " with error=", err)
            return
        }
        defer file.Close()

        writer := bufio.NewWriter(file)
        defer writer.Flush()

        for task := range in {
            select {
            case <-ctx.Done():
                log.Println("Stopping writing to file due to ctx cancelled")
                return
            default:
            }
            _, err := writer.WriteString(task.Content + "\n\n" + "\n\n<eos>\n")
            if err != nil {
                log.Println("Error writing to file=", w.Filepath, " with error=", err)
                return
            }
            select {
            case <-ctx.Done():
                log.Println("Stopping writing to file due to ctx cancelled")
                return
            case out <- task:
            }

        }
        log.Println("Finished writing to file=", w.Filepath)
    }()
    return out
}

func (a *AnalyzeDataset) Stage(ctx context.Context, in chan Task) chan Task {
    out := make(chan Task)
    go func() {
        defer close(out)

        // DRAIN THE CHANNEL
        // We must wait for the previous stage (Write) to finish writing everything.
        // We act as a "sink" here.
        for task := range in {
            select {
            case <-ctx.Done():
                return
            default:
                log.Printf("Analyzing Dataset: Received Task ID %d\n", task.ID)
                out <- task
            }
        }

        // RUN PYTHON SCRIPT
        // The channel 'in' is closed, meaning the file is fully written.
        log.Println("Pipeline finished. Triggering Python analysis...")

        cmd := exec.Command(a.PythonPath, "analyze_dataset.py", a.Filepath)

        // Connect Python output to Go's stdout
        cmd.Stdout = os.Stdout
        cmd.Stderr = os.Stderr

        if err := cmd.Run(); err != nil {
            log.Printf("Error running analysis script: %v\n", err)
        }
    }()
    return out
}

func (w *WriteQA) Stage(ctx context.Context, in chan Task) chan Task {
    out := make(chan Task)
    go func() {
        defer close(out)

        file, err := os.Create(w.Filepath)
        if err != nil {
            log.Println("Error creating output file:", err)
            return
        }
        defer file.Close()

        writer := bufio.NewWriter(file)
        defer writer.Flush()

        count := 0
        for task := range in {
            select {
            case <-ctx.Done():
                return
            default:
                // Write formatted content
                _, err := writer.WriteString(task.Content)
                if err != nil {
                    log.Println("Error writing to file:", err)
                    return
                }
                count++
            }
        }
        log.Println("Total records written:", count)
    }()
    return out
}
