// Command gendata writes the seeded S5.2 load dataset (loadtest/dataset)
// for the k6 scenarios and prints its summary as JSON on stdout: seed, run,
// counts and both hashes, which the load report records.
//
//	go run ./loadtest/cmd/gendata -seed 52 -run S5.2-smoke-<UTC> -ops 2000 -out /tmp/ds.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/dataset"
)

func main() {
	seed := flag.Int64("seed", 52, "seed: fixes every payload and the order of operations")
	run := flag.String("run", "", "run name (required): makes the Idempotency-Keys unique to this run")
	ops := flag.Int("ops", 20_000, "number of operations")
	out := flag.String("out", "", "output file (required)")
	flag.Parse()
	if *run == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "gendata: -run and -out are required")
		os.Exit(2)
	}
	if err := write(*seed, *run, *ops, *out); err != nil {
		fmt.Fprintln(os.Stderr, "gendata:", err)
		os.Exit(1)
	}
}

func write(seed int64, run string, ops int, out string) error {
	d, err := dataset.Generate(dataset.Config{Seed: seed, Run: run, Ops: ops})
	if err != nil {
		return err
	}
	f, err := os.Create(out) //nolint:gosec // the operator names the output file
	if err != nil {
		return err
	}
	if err := d.Write(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"seed": d.Seed, "run": d.Run, "ops": d.Counts.Ops, "requests": d.Counts.Requests, "resources": d.Counts.Resources,
		"sha256": d.SHA256(), "payloadSha256": d.PayloadSHA256(), "mix": d.Mix, "path": out,
	})
}
