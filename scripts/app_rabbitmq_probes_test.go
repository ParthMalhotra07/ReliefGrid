//go:build integration

package scripts_test

import (
	"testing"
)

// The broker's probes must not cost a CPU-bound VM boot each. Each
// rabbitmq-diagnostics call starts a full Erlang CLI VM (about 1.45
// CPU-seconds measured on kind); at readiness every 5 s plus liveness every
// 10 s that was about 0.45 CPU at idle, and under a 750m limit one call took
// as long as its 5 s timeout, so idle liveness probes timed out. Startup and
// readiness are TCP checks on the AMQP port. Liveness is an HTTP GET answered
// by the broker's own VM (a TCP check is not enough: the kernel completes a
// handshake on a listening socket even when the VM is frozen). No probe may
// exec a command.
func TestAppRabbitMQProbesAreLightweight(t *testing.T) {
	var broker map[string]any
	for _, d := range renderApp(t, "-f", "../deploy/helm/reliefgrid/values-app-kind.yaml", "--set", "storage.enabled=false") {
		if d["kind"] == "StatefulSet" && appField(t, d, "metadata", "name") == "reliefgrid-rabbitmq" {
			broker = d
		}
	}
	if broker == nil {
		t.Fatal("no reliefgrid-rabbitmq StatefulSet rendered")
	}
	c := appField(t, broker, "spec", "template", "spec", "containers").([]any)[0].(map[string]any)
	probe := func(name string) map[string]any {
		p, ok := c[name].(map[string]any)
		if !ok {
			t.Fatalf("broker has no %s", name)
		}
		if _, ok := p["exec"]; ok {
			t.Errorf("%s execs a command (%v): each run boots an Erlang VM", name, p["exec"])
		}
		return p
	}
	for _, name := range []string{"startupProbe", "readinessProbe"} {
		p := probe(name)
		if port := appField(t, p, "tcpSocket", "port"); port != "amqp" && port != 5672 {
			t.Errorf("%s: tcpSocket port %v, want the AMQP port", name, port)
		}
	}
	live := probe("livenessProbe")
	if got := appField(t, live, "httpGet", "port"); got != "management" && got != 15672 {
		t.Errorf("livenessProbe: httpGet port %v, want the management listener (answered by the broker's VM)", got)
	}
	// First boot (definitions import, quorum queues) gets minutes, not seconds.
	st := probe("startupProbe")
	if budget := appField(t, st, "periodSeconds").(int) * appField(t, st, "failureThreshold").(int); budget < 180 {
		t.Errorf("startupProbe allows %d s for first boot, want at least 180", budget)
	}
}
