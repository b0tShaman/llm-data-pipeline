package main

import (
	"context"
	"encoding/xml"
	"fmt"
	"html"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// StreamXMLFiles: Finds all .xml files in a directory and emits them as tasks
type StreamXMLFiles struct {
	Directory string
}

// ProcessStackExchangeXML: Parses XML, links Q&A, and formats text
type ProcessStackExchangeXML struct {
	MinScore int
}

type Row struct {
	Id               string `xml:"Id,attr"`
	PostTypeId       string `xml:"PostTypeId,attr"` // 1=Question, 2=Answer
	Body             string `xml:"Body,attr"`
	Title            string `xml:"Title,attr"`
	AcceptedAnswerId string `xml:"AcceptedAnswerId,attr"`
	Score            int    `xml:"Score,attr"`
}

func (s *StreamXMLFiles) Stage(ctx context.Context, in chan Task) chan Task {
	out := make(chan Task)
	go func() {
		defer close(out)

		log.Println("Scanning directory:", s.Directory)
		files, err := filepath.Glob(filepath.Join(s.Directory, "*.xml"))
		if err != nil {
			log.Println("Error finding XML files:", err)
			return
		}

		if len(files) == 0 {
			log.Println("No .xml files found in", s.Directory)
			return
		}

		id := 0
		for _, file := range files {
			select {
			case <-ctx.Done():
				return
			case out <- Task{ID: id, Source: file}:
				id++
			}
		}
		log.Printf("Found %d files to process.\n", len(files))
	}()
	return out
}

func (p *ProcessStackExchangeXML) Stage(ctx context.Context, in chan Task) chan Task {
	out := make(chan Task)

	cleanText := func(input string) string {
		text := html.UnescapeString(input)
		text = tagRegex.ReplaceAllString(text, " ") // Remove HTML tags

		// Use the global regexes to preserve paragraph/code block structure
		s := reSpace.ReplaceAllString(text, " ")
		return strings.TrimSpace(reNewlines.ReplaceAllString(s, "\n\n"))
	}

	parseAndLinkXML := func(filename string, minScore int) []string {
		xmlFile, err := os.Open(filename)
		if err != nil {
			log.Printf("Error opening %s: %v", filename, err)
			return nil
		}
		defer xmlFile.Close()

		decoder := xml.NewDecoder(xmlFile)

		questions := make(map[string]*Row)
		answers := make(map[string]string)
		var results []string

		// Stream XML
		for {
			t, _ := decoder.Token()
			if t == nil {
				break
			}

			switch se := t.(type) {
			case xml.StartElement:
				if se.Name.Local == "row" {
					var row Row
					if err := decoder.DecodeElement(&row, &se); err != nil {
						continue
					}

					// Is Question?
					if row.PostTypeId == "1" && row.AcceptedAnswerId != "" {
						questions[row.Id] = &row
					}
					// Is Answer?
					if row.PostTypeId == "2" && row.Score >= minScore {
						answers[row.Id] = row.Body
					}
				}
			}
		}

		// Link
		for _, q := range questions {
			if ansBody, exists := answers[q.AcceptedAnswerId]; exists {
				qText := cleanText(q.Title + " " + q.Body)
				aText := cleanText(ansBody)

				// Format: <user>: ... <bot>: ...
				formatted := fmt.Sprintf("<user>: %s\n<bot>: %s\n<eos>\n", qText, aText)
				results = append(results, formatted)
			}
		}
		return results
	}

	go func() {
		defer close(out)

		// Process files sequentially to save RAM
		for task := range in {
			select {
			case <-ctx.Done():
				return
			default:
			}

			log.Println("Processing File:", task.Source)

			// We process the ENTIRE file here and emit multiple tasks (one per Q&A pair)
			pairs := parseAndLinkXML(task.Source, p.MinScore)

			for i, content := range pairs {
				select {
				case <-ctx.Done():
					return
				case out <- Task{
					ID:      (task.ID * 1000000) + i, // Unique ID generation
					Source:  task.Source,
					Content: content,
				}:
				}
			}
			log.Printf("Finished %s. Extracted %d pairs.\n", filepath.Base(task.Source), len(pairs))
		}
	}()
	return out
}