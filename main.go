package main

import (
	"context"
	"log"
	"regexp"
)

// --- GLOBALS ---
var mode string = "wiki" // Options: "wiki", "reddit", "stack"
var pythonCmd string = "python"

var (
	reSpace    = regexp.MustCompile(`[ \t]+`)
	reNewlines = regexp.MustCompile(`\n{3,}`)
	tagRegex   = regexp.MustCompile(`<[^>]*>`)
)

func main() {
	ctx := context.Background()

	var stages []Pipeline

	switch mode {
	case "wiki":
		stages = []Pipeline{
			&StreamURL{Filepath: "urls.txt"},
			&DownloadURL{NumWorkers: 20},
			&ExtractTextWiki{},
			&GenerateQAWiki{ModelName: "gemma3:1b"},
			&WriteQA{Filepath: "analyze_wiki.txt"},
			&AnalyzeDataset{Filepath: "analyze_wiki.txt", PythonPath: pythonCmd},
			// Create the final dataset with FlattenLines
			&FlattenLines{SkipEOS: true},
			&WriteQA{Filepath: "dataset_wiki.txt"},
		}
	case "reddit":
		stages = []Pipeline{
			&FetchLinks{
				CCIndex:      "CC-MAIN-2023-50",
				NumPages:     15,
				Label:        "Reddit",
				QueryPattern: "*.reddit.com/r/*/comments/*/*/*", // Catch all, then convert to old.reddit
				Target:       5000,
			},
			&DownloadURL{NumWorkers: 20},
			&ExtractTextReddit{},
			&WriteQA{Filepath: "analyze_reddit.txt"},
			&AnalyzeDataset{Filepath: "analyze_reddit.txt", PythonPath: pythonCmd},
			// Create the final dataset with FlattenLines
			&FlattenLines{SkipEOS: true},
			&WriteQA{Filepath: "dataset_reddit.txt"},
		}
	case "stack":
		stages = []Pipeline{
			&StreamXMLFiles{Directory: "./xml_dump"},
			&ProcessStackExchangeXML{MinScore: 1},
			&WriteQA{Filepath: "analyze_stackoverflow.txt"},
			&AnalyzeDataset{Filepath: "analyze_stackoverflow.txt", PythonPath: pythonCmd},
			// Create the final dataset with FlattenLines
			&FlattenLines{SkipEOS: true},
			&WriteQA{Filepath: "dataset_stack.txt"},
		}
	}

	// Run
	log.Printf("Starting Pipeline in %s mode...\n", mode)
	finalChan := RunPipeline(ctx, stages...)

	for task := range finalChan {
		log.Printf("[%s] Processed Task ID: %d | Size: %d bytes", mode, task.ID, len(task.Content))
	}
}
