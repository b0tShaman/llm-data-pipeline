package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
)

type ExtractTextWiki struct {
}

// GenerateQAWiki is a pipeline stage that converts Wikipedia paragraphs into Q&A pairs.
type GenerateQAWiki struct {
	ModelName string // Use "gemma3:1b"
}

func (e *ExtractTextWiki) Stage(ctx context.Context, in chan Task) chan Task {
	out := make(chan Task)

	// Replace multiple spaces with one, but keep newlines intact
	cleanText := func(input string) string {
		s := reSpace.ReplaceAllString(input, " ")
		return strings.TrimSpace(reNewlines.ReplaceAllString(s, "\n\n"))
	}

	// --- Stop Reading at Footer ---
	checkID := func(s *goquery.Selection) bool {
		id, exists := s.Attr("id")
		if !exists {
			return false
		}
		id = strings.ToLower(id)
		return strings.Contains(id, "see_also") ||
			strings.Contains(id, "references") ||
			strings.Contains(id, "notes") ||
			strings.Contains(id, "external_links") ||
			strings.Contains(id, "bibliography") ||
			strings.Contains(id, "further_reading")
	}

	isFooterHeader := func(s *goquery.Selection) bool {
		if checkID(s) {
			return true
		}
		if checkID(s.Parent()) {
			return true
		}
		if checkID(s.Find(".mw-headline")) {
			return true
		}

		text := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, s.Text())

		switch text {
		case "seealso", "references", "notes", "externallinks", "bibliography", "furtherreading", "sources", "citations":
			return true
		}
		return false
	}

	go func() {
		defer close(out)
		for task := range in {
			select {
			case <-ctx.Done():
				log.Println("Stopping text extraction due to ctx cancelled")
				return
			default:
			}

			content := []byte(task.Content)
			doc, err := goquery.NewDocumentFromReader(bytes.NewReader(content))
			if err != nil {
				log.Println("goquery NewDocumentFromReader failed for task ID=", task.ID, " with error=", err)
				return
			}

			var sb strings.Builder

			// ---------------------------------------------------------
			// 0. Extract Main Page Title (The H1)
			// ---------------------------------------------------------
			// Wikipedia titles usually have the ID "firstHeading"
			pageTitle := doc.Find("#firstHeading").First()

			// Fallback for some skins or mobile views
			if pageTitle.Length() == 0 {
				pageTitle = doc.Find("h1").First()
			}

			titleText := cleanText(pageTitle.Text())
			if titleText != "" {
				// Formatting it as an H1 Markdown header
				sb.WriteString("# " + titleText + "\n\n")
			}

			// 1. Target the main content
			selection := doc.Find(".mw-parser-output")
			if selection.Length() == 0 {
				selection = doc.Find("#mw-content-text")
			}

			// 2. Remove Junk
			selection.Find(".mw-editsection, #toc, .toc, .infobox, .thumb, .reference, .noprint, .refbegin, .reflist, script, style, table, .mw-empty-elt").Remove()

			stopReading := false
			contentTags := "h2, h3, h4, h5, h6, p, ul, ol, dl, blockquote"

			selection.Find(contentTags).EachWithBreak(func(i int, s *goquery.Selection) bool {
				tag := goquery.NodeName(s)

				// --- HEADER HANDLING ---
				if tag == "h2" {
					if isFooterHeader(s) {
						stopReading = true
						return false
					}
					text := cleanText(s.Text())
					if text != "" {
						sb.WriteString("\n\n## " + text + "\n")
					}
					return true
				}

				// --- SUB-HEADERS ---
				if tag == "h3" || tag == "h4" || tag == "h5" || tag == "h6" {
					text := cleanText(s.Text())
					if text != "" {
						sb.WriteString("\n### " + text + "\n")
					}
					return true
				}

				if stopReading {
					return false
				}

				// --- BODY CONTENT ---
				text := cleanText(s.Text())
				if text != "" {
					sb.WriteString(text + "\n\n")
				}

				return true
			})

			select {
			case <-ctx.Done():
				log.Println("Stopping text extraction due to ctx cancelled")
				return
			case out <- Task{ID: task.ID, URL: task.URL, Content: strings.TrimSpace(sb.String())}:
			}
		}
	}()
	return out
}

// Stage implements the Pipeline interface logic.
func (g *GenerateQAWiki) Stage(ctx context.Context, in chan Task) chan Task {
	out := make(chan Task)

	// cleanParagraph removes Markdown headers from a text block.
	cleanParagraph := func(input string) string {
		lines := strings.Split(input, "\n")
		var result []string
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			// Skip lines that are Markdown headers (# Title)
			if strings.HasPrefix(trimmed, "#") || trimmed == "" {
				continue
			}
			result = append(result, trimmed)
		}
		return strings.Join(result, " ")
	}

	// askGemma communicates with Ollama's /api/generate endpoint.
	askGemma := func(ctx context.Context, paragraph string) (string, error) {
		// Custom prompt for Story Mode training
		prompt := fmt.Sprintf(`Read this text:
        "%s"

        Task: Write a single, short question that this text answers. 
        Rules:
        1. Do not mention "the text" or "the paragraph" in the question.
        2. Do not include the answer.
        3. Just output the question string.`, paragraph)

		// Ollama API Payload
		payload := map[string]any{
			"model":  g.ModelName,
			"prompt": prompt,
			"stream": false, // Set to false to get a single JSON response
			"options": map[string]any{
				"temperature": 0.3, // Low temperature for more consistent questions
			},
		}

		jsonData, _ := json.Marshal(payload)

		// Use a client with a longer timeout because LLM generation takes time
		client := &http.Client{Timeout: 60 * time.Second}

		req, err := http.NewRequestWithContext(ctx, "POST", "http://localhost:11434/api/generate", bytes.NewBuffer(jsonData))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("ollama returned status: %d", resp.StatusCode)
		}

		var result struct {
			Response string `json:"response"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return "", err
		}

		return strings.TrimSpace(result.Response), nil
	}

	go func() {
		defer close(out)
		for task := range in {
			// Split by double newline to separate paragraphs
			blocks := strings.Split(task.Content, "\n\n")
			var finalQAContent strings.Builder

			count := 0
			total := len(blocks)

			for i, block := range blocks {
				// 1. Clean the block: remove headers and trim whitespace
				cleanPara := cleanParagraph(block)

				// 2. Skip if it's too short to be a useful training sample
				if len(cleanPara) < 150 {
					continue
				}

				count++
				// Confirm we are about to call the LLM
				log.Printf("[Task %d] Processing block %d/%d (Length: %d chars)...", task.ID, i+1, total, len(cleanPara))

				start := time.Now() // Timer start

				// 3. Send to Gemma 3:1b via local Ollama API
				qaResponse, err := askGemma(ctx, cleanPara)
				if err != nil {
					log.Printf("[Task %d] Gemma Error: %v", task.ID, err)
					continue
				}

				// Confirm the LLM finished
				duration := time.Since(start)
				log.Printf("[Task %d] Block %d DONE in %v", task.ID, i+1, duration)

				// 2. Clean the question string
				// Sometimes models add quotes or "Here is a question:" prefixes.
				qaResponse = strings.Trim(qaResponse, "\"")
				qaResponse = strings.TrimPrefix(qaResponse, "Question: ")
				qaResponse = strings.TrimSpace(qaResponse)

				// 3. Construct the EXACT format you want in Go
				// We attach the <eos> tag here manually.
				formattedEntry := fmt.Sprintf("<user>: %s\n<bot>: %s <eos>\n", qaResponse, cleanPara)

				// 4. Append the formatted result
				finalQAContent.WriteString(formattedEntry)
			}

			// Update the task content with the new Q&A pairs
			task.Content = strings.TrimSpace(finalQAContent.String()) + "\n"

			select {
			case <-ctx.Done():
				return
			case out <- task:
			}
		}
	}()
	return out
}
