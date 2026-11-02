package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/ParthMalhotra07/ReliefGrid/chaos/judge"
)

// judgeCmd is invcheck judge --input FILE|-: the S4.3 pass criteria
// (chaos/judge) over one run's observations. It needs no coordinator. Prints
// the result as JSON; exit 0 passed, 1 failed, 2 no verdict, 64 usage.
func judgeCmd(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("invcheck judge", flag.ContinueOnError)
	fs.SetOutput(stderr)
	input := fs.String("input", "", "the run's observations as JSON (chaos/judge.Input); - for stdin")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *input == "" {
		_, _ = fmt.Fprintln(stderr, "invcheck: judge needs --input FILE or --input -")
		return exitUsage
	}
	r := stdin
	if *input != "-" {
		f, err := os.Open(*input)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "invcheck: %v\n", err)
			return exitIncomplete
		}
		defer func() { _ = f.Close() }()
		r = f
	}
	// Fields judge doesn't use are ignored: verify and check JSON are passed
	// whole. A fact that is missing fails its criterion rather than passing.
	var in judge.Input
	if err := json.NewDecoder(r).Decode(&in); err != nil {
		_, _ = fmt.Fprintf(stderr, "invcheck: judge input: %v\n", err)
		return exitIncomplete
	}
	if in.Experiment == "" {
		_, _ = fmt.Fprintln(stderr, "invcheck: judge input names no experiment")
		return exitIncomplete
	}
	res := judge.Judge(in)
	writeJSON(stdout, res)
	switch {
	case res.NoVerdict:
		return exitIncomplete
	case !res.Pass:
		return exitViolations
	}
	return exitClean
}
