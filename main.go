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
    tagRegex = regexp.MustCompile(`<[^>]*>`)
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
            &WritePlainText{Filepath: "dataset_wiki.txt"},
            &AnalyzeDataset{Filepath: "dataset_wiki.txt", PythonPath: pythonCmd},
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
            &WriteQA{Filepath: "dataset_reddit.txt"},
            &AnalyzeDataset{Filepath: "dataset_reddit.txt", PythonPath: pythonCmd},
        }
    case "stack":
        stages = []Pipeline{
            &StreamXMLFiles{Directory: "./xml_dump"},
            &ProcessStackExchangeXML{MinScore: 1},
            &WriteQA{Filepath: "dataset_stackoverflow.txt"},
            &AnalyzeDataset{Filepath: "dataset_stackoverflow.txt", PythonPath: pythonCmd},
        }
    }

    // Run
    log.Printf("Starting Pipeline in %s mode...\n", mode)
    finalChan := RunPipeline(ctx, stages...)

    for task := range finalChan {
        log.Printf("[%s] Processed Task ID: %d | Size: %d bytes", mode, task.ID, len(task.Content))
    }
}