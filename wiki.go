package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"unicode"

	"github.com/PuerkitoBio/goquery"
)

type ExtractTextWiki struct {
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

