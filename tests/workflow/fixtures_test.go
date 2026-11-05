package workflow

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var scheduleHeading = regexp.MustCompile(`(?m)^## (SCH\d{2}[ab]?) — (.+)$`)

func TestFixturesMatchSchedulesMarkdown(t *testing.T) {
	markdown, err := os.ReadFile("schedules.md")
	if err != nil {
		t.Fatal(err)
	}

	document := strings.ReplaceAll(string(markdown), "\r\n", "\n")
	headings := scheduleHeading.FindAllStringSubmatchIndex(document, -1)
	if len(headings) != len(Schedules) {
		t.Fatalf("schedules.md has %d schedules; fixtures.go has %d", len(headings), len(Schedules))
	}

	for index, fixture := range Schedules {
		fixture := fixture
		heading := headings[index]
		sectionEnd := len(document)
		if index+1 < len(headings) {
			sectionEnd = headings[index+1][0]
		}
		section := document[heading[1]:sectionEnd]
		code := document[heading[2]:heading[3]]
		name := document[heading[4]:heading[5]]

		t.Run(code, func(t *testing.T) {
			if !strings.HasPrefix(fixture.Tag, code+"_") {
				t.Errorf("fixture tag %q does not match Markdown heading %s", fixture.Tag, code)
			}
			if fixture.Name != name {
				t.Errorf("fixture name %q; Markdown name %q", fixture.Name, name)
			}

			sourceLine := `**Source:** §27 "` + fixture.Source + `"`
			if !strings.Contains(section, sourceLine) {
				t.Errorf("Markdown source does not match fixture source %q", fixture.Source)
			}

			rows, columns := expectedFinalState(t, section)
			for _, state := range fixture.FinalState {
				row, ok := rows[state.Entity]
				if !ok {
					t.Errorf("Markdown expected final state has no row for %s", state.Entity)
					continue
				}

				wantStatus := fmt.Sprint(state.ReqStatus)
				if wantStatus == "" {
					wantStatus = fmt.Sprint(state.ResStatus)
				}
				if column, ok := columns["status"]; ok && wantStatus != "" && row[column] != wantStatus {
					t.Errorf("%s status: fixture %s; Markdown %s", state.Entity, wantStatus, row[column])
				}

				if column, ok := columns["activeclaim"]; ok {
					got := row[column]
					if got == "—" || got == "-" {
						got = ""
					}
					if got != state.ActiveClaim {
						t.Errorf("%s activeClaim: fixture %q; Markdown %q", state.Entity, state.ActiveClaim, got)
					}
				}

				if column, ok := columns["version"]; ok {
					got, err := strconv.Atoi(row[column])
					if err != nil {
						t.Errorf("%s Markdown version %q is not an integer", state.Entity, row[column])
					} else if got != state.Version {
						t.Errorf("%s version: fixture %d; Markdown %d", state.Entity, state.Version, got)
					}
				}
			}
		})
	}
}

func expectedFinalState(t *testing.T, section string) (map[string][]string, map[string]int) {
	t.Helper()

	const marker = "### Expected final state"
	markerAt := strings.Index(section, marker)
	if markerAt < 0 {
		t.Fatal("missing expected final state section")
	}

	lines := strings.Split(section[markerAt+len(marker):], "\n")
	var table [][]string
	for _, line := range lines {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			if len(table) > 0 {
				break
			}
			continue
		}
		cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
		for index := range cells {
			cells[index] = strings.TrimSpace(cells[index])
		}
		table = append(table, cells)
	}
	if len(table) < 3 {
		t.Fatal("expected final state table is missing or empty")
	}

	columns := make(map[string]int, len(table[0]))
	for index, name := range table[0] {
		columns[strings.ToLower(name)] = index
	}
	entityColumn, ok := columns["entity"]
	if !ok {
		t.Fatal("expected final state table has no Entity column")
	}

	rows := make(map[string][]string, len(table)-2)
	for _, row := range table[2:] {
		if len(row) != len(table[0]) {
			t.Fatalf("expected final state row has %d cells; want %d: %v", len(row), len(table[0]), row)
		}
		entity := strings.Fields(row[entityColumn])[0]
		rows[entity] = row
	}
	return rows, columns
}
