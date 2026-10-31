//go:build integration

package scripts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

// Decode the real rendered chart strictly, then prove the label/selector values
// and RF3 counts are preserved. Repeating a mapping key is invalid YAML even
// when the repeated value is identical; permissive consumers can hide it.
func TestStorageChartUniquePodLabelsPreserveSelectors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "helm", "template", "storage", "../deploy/helm/storage", "--namespace", "reliefgrid-label-test")
	var diagnostics bytes.Buffer
	cmd.Stderr = &diagnostics
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("render storage chart: %v: %s", err, diagnostics.Bytes())
	}
	decoder := yaml.NewDecoder(bytes.NewReader(output))
	seen := map[string]bool{}
	for {
		var d struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				Replicas int `yaml:"replicas"`
				Selector struct {
					Labels map[string]string `yaml:"matchLabels"`
				} `yaml:"selector"`
				Template struct {
					Metadata struct {
						Labels map[string]string `yaml:"labels"`
					} `yaml:"metadata"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		err := decoder.Decode(&d)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("storage chart must render unique YAML mapping keys: %v", err)
		}
		if d.Kind != "StatefulSet" {
			continue
		}
		shard, err := strconv.Atoi(strings.TrimPrefix(d.Metadata.Name, "storage-shard-"))
		if err != nil || shard < 0 || shard > 2 || seen[d.Metadata.Name] {
			t.Fatalf("unexpected or duplicate shard StatefulSet %s", d.Metadata.Name)
		}
		seen[d.Metadata.Name] = true
		if d.Spec.Replicas != 3 {
			t.Errorf("%s replicas=%d want 3", d.Metadata.Name, d.Spec.Replicas)
		}
		want := map[string]string{"app.kubernetes.io/name": "storage", "app.kubernetes.io/component": "storagenode", "reliefgrid.io/shard": strconv.Itoa(shard)}
		if len(d.Spec.Selector.Labels) != len(want) {
			t.Errorf("%s selector changed: %v", d.Metadata.Name, d.Spec.Selector.Labels)
		}
		for key, value := range want {
			if d.Spec.Selector.Labels[key] != value || d.Spec.Template.Metadata.Labels[key] != value {
				t.Errorf("%s %s: selector=%q pod=%q want=%q", d.Metadata.Name, key, d.Spec.Selector.Labels[key], d.Spec.Template.Metadata.Labels[key], value)
			}
		}
		if d.Spec.Template.Metadata.Labels["app.kubernetes.io/instance"] != "storage" {
			t.Errorf("%s lost release label", d.Metadata.Name)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("rendered %d shard StatefulSets, want 3", len(seen))
	}
}
