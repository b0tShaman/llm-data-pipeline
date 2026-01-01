package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// Stage to extract Reddit Q&A pairs
type ExtractTextReddit struct{}

type CCRecord struct {
	URL    string `json:"url"`
	Status string `json:"status"`
}

type FetchLinks struct {
	CCIndex      string
	NumPages     int
	Label        string
	QueryPattern string
	Target       int
}

func (f *FetchLinks) Stage(ctx context.Context, in chan Task) chan Task {
	out := make(chan Task)
	go func() {
		defer close(out)
		uniqueURLs := make(map[string]bool)
		var count int

		// queryPattern will be something like "*.quora.com/*/answer/*"
		apiURLBase := fmt.Sprintf("http://index.commoncrawl.org/%s-index?url=%s&output=json", f.CCIndex, f.QueryPattern)

		for p := 0; p < f.NumPages; p++ {
			fmt.Printf("[*] %s: Scanning Page %d (Found: %d/%d)\n", f.Label, p, count, f.Target)

			fullURL := fmt.Sprintf("%s&page=%d", apiURLBase, p)
			resp, err := http.Get(fullURL)

			// If 404, we've exhausted the index for this pattern
			if err != nil || resp.StatusCode == 404 {
				break
			}
			if resp.StatusCode != 200 {
				time.Sleep(2 * time.Second)
				continue
			}

			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				var rec CCRecord
				if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
					continue
				}

				// Since we queried for the pattern, we know it's likely an answer
				if rec.Status == "200" && !uniqueURLs[rec.URL] {
					uniqueURLs[rec.URL] = true
					select {
					case <-ctx.Done():
						resp.Body.Close()
						return
					default:
						// Force old reddit for easier parsing
						rec.URL = strings.Replace(rec.URL, "www.reddit.com", "old.reddit.com", 1)
						out <- Task{ID: count, URL: rec.URL}
						count++
					}
				}

				if count >= f.Target {
					resp.Body.Close()
					// return results
					return
				}
			}
			resp.Body.Close()
			time.Sleep(1 * time.Second)
		}

	}()
	return out
}

func (e *ExtractTextReddit) Stage(ctx context.Context, in chan Task) chan Task {
	out := make(chan Task)

	cleanText := func(input string) string {
		s := reSpace.ReplaceAllString(input, " ")
		return strings.TrimSpace(reNewlines.ReplaceAllString(s, "\n\n"))
	}

	go func() {
		defer close(out)
		for task := range in {
			// TODO : Add standard checks/doc creation here

			doc, _ := goquery.NewDocumentFromReader(bytes.NewReader([]byte(task.Content)))

			// 1. Extract Question (Title + Selftext)
			// On old.reddit.com, the main post is in div.sitetable -> div.thing
			title := doc.Find("a.title").First().Text()
			body := doc.Find("div.expando div.usertext-body").First().Text()

			questionText := strings.TrimSpace(title + "\n" + body)

			// 2. Extract Top Answer
			// We only want the top comment to act as the "bot"
			// '.commentarea > .sitetable > .thing' selects top-level comments only
			topComment := doc.Find("div.commentarea > div.sitetable > div.thing").First()
			answerText := topComment.Find("div.usertext-body").First().Text()

			// Basic validation to ensure we have content
			if len(questionText) > 10 && len(answerText) > 10 {
				formatted := fmt.Sprintf("<user>: %s\n<bot>: %s\n<eos>\n",
					cleanText(questionText),
					cleanText(answerText))

				select {
				case <-ctx.Done():
					return
				case out <- Task{ID: task.ID, Content: formatted}:
				}
			}
		}
	}()
	return out
}