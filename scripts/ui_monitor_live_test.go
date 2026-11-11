//go:build monitor

package scripts_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func monitorOpen(t *testing.T, key string) *amqp.Channel {
	t.Helper()
	address := os.Getenv(key)
	if address == "" {
		t.Fatalf("%s required", key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	conn, err := amqp.DialConfig(address, amqp.Config{Heartbeat: 5 * time.Second, Properties: amqp.Table{"connection_name": t.Name()}, Dial: func(network, address string) (net.Conn, error) {
		transport, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if err := transport.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
			_ = transport.Close()
			return nil, err
		}
		return transport, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	channel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	return channel
}
func monitorPublish(t *testing.T, ch *amqp.Channel) {
	t.Helper()
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(ctx, "", "matching.requests", true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: []byte("ui-monitor-upgrade-fixture")}); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-confirms:
		if !result.Ack {
			t.Fatal("application publish was not confirmed")
		}
	case <-ctx.Done():
		t.Fatal("application confirm missing")
	}
}
func TestMonitorSeed(t *testing.T) { monitorPublish(t, monitorOpen(t, "MONITOR_APP_AMQP")) }

func TestUIBrokerMonitorLive(t *testing.T) {
	origin, err := url.Parse(os.Getenv("MONITOR_HTTP"))
	if err != nil || origin.User == nil {
		t.Fatal("monitor HTTP identity required")
	}
	user := origin.User.Username()
	password, _ := origin.User.Password()
	origin.User = nil
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	callAs := func(t *testing.T, username, secret, method, path, body string) (int, []byte) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(origin.String(), "/")+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth(username, secret)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, data
	}
	call := func(method, path, body string) (int, []byte) {
		return callAs(t, user, password, method, path, body)
	}
	appOrigin, err := url.Parse(os.Getenv("MONITOR_APP_AMQP"))
	if err != nil || appOrigin.User == nil {
		t.Fatal("application AMQP identity required for privacy controls")
	}
	appUser := appOrigin.User.Username()
	appPassword, ok := appOrigin.User.Password()
	if !ok || appUser == user {
		t.Fatal("privacy controls require distinct application administrator identity")
	}
	t.Run("node-details-denied", func(t *testing.T) {
		status, data := callAs(t, appUser, appPassword, "GET", "/api/nodes", "")
		if status != 200 {
			t.Fatalf("administrator node control returned %d", status)
		}
		var nodes []struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(data, &nodes); err != nil || len(nodes) == 0 {
			t.Fatal("administrator did not observe an existing broker node")
		}
		status, _ = callAs(t, user, password, "GET", "/api/nodes", "")
		if status != 401 && status != 403 {
			t.Errorf("monitor node listing was not denied: %d", status)
		}
		path := "/api/nodes/" + url.PathEscape(nodes[0].Name)
		status, _ = callAs(t, appUser, appPassword, "GET", path, "")
		if status != 200 {
			t.Fatalf("administrator existing node detail control returned %d", status)
		}
		status, _ = callAs(t, user, password, "GET", path, "")
		if status != 401 && status != 403 && status != 404 {
			t.Errorf("monitor existing node detail was not denied: %d", status)
		}
	})
	t.Run("other-user-connections-denied", func(t *testing.T) {
		// This connection stays open until the subtest ends. A successful
		// administrator control proves a later 404 is authorization, not absence.
		monitorOpen(t, "MONITOR_APP_AMQP")
		type connection struct {
			Name             string `json:"name"`
			User             string `json:"user"`
			ClientProperties struct {
				ConnectionName string `json:"connection_name"`
			} `json:"client_properties"`
		}
		name := ""
		deadline := time.Now().Add(5 * time.Second)
		for name == "" {
			status, data := callAs(t, appUser, appPassword, "GET", "/api/connections", "")
			if status != 200 {
				t.Fatalf("administrator connection control returned %d", status)
			}
			var connections []connection
			if err := json.Unmarshal(data, &connections); err != nil {
				t.Fatal("invalid administrator connection response")
			}
			for _, candidate := range connections {
				if candidate.User == appUser && candidate.ClientProperties.ConnectionName == t.Name() {
					name = candidate.Name
				}
			}
			if name == "" {
				if !time.Now().Before(deadline) {
					t.Fatal("administrator did not observe the live other-user connection")
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		path := "/api/connections/" + url.PathEscape(name)
		status, _ := callAs(t, appUser, appPassword, "GET", path, "")
		if status != 200 {
			t.Fatalf("administrator existing connection detail control returned %d", status)
		}
		status, data := callAs(t, user, password, "GET", "/api/connections", "")
		if status == 200 {
			var connections []connection
			if err := json.Unmarshal(data, &connections); err != nil {
				t.Fatal("invalid monitor connection response")
			}
			for _, candidate := range connections {
				if candidate.Name == name || candidate.User != user {
					t.Error("monitor connection listing exposed another user's connection")
				}
			}
		} else if status != 401 && status != 403 {
			t.Errorf("monitor connection listing returned unexpected status: %d", status)
		}
		status, _ = callAs(t, user, password, "GET", path, "")
		if status != 401 && status != 403 && status != 404 {
			t.Errorf("monitor existing other-user connection detail was not denied: %d", status)
		}
		t.Log("administrator confirmed live other-user connection before monitor privacy checks")
	})
	for _, queue := range []string{"matching.requests", "matching.dead"} {
		started := time.Now()
		deadline := started.Add(15 * time.Second)
		for {
			status, data := call("GET", "/api/queues/%2F/"+queue, "")
			if status != 200 {
				t.Fatalf("monitor queue stats returned %d", status)
			}
			var stats struct {
				Ready   *int64 `json:"messages_ready"`
				Unacked *int64 `json:"messages_unacknowledged"`
			}
			if err := json.Unmarshal(data, &stats); err != nil {
				t.Fatal(err)
			}
			if stats.Ready != nil && stats.Unacked != nil {
				if *stats.Ready < 0 || *stats.Unacked < 0 {
					t.Fatal("negative queue statistics")
				}
				t.Logf("observed actual queue counters after %s", time.Since(started).Round(time.Millisecond))
				break
			}
			if !time.Now().Before(deadline) {
				t.Fatal("queue counters not observed within 15s; missing is not zero")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	for _, attempt := range []struct{ method, path, body string }{{"DELETE", "/api/queues/%2F/matching.dead", ""}, {"PUT", "/api/users/monitor-forbidden", `{"password":"synthetic-denied-fixture","tags":"administrator"}`}} {
		status, _ := call(attempt.method, attempt.path, attempt.body)
		if status != 401 && status != 403 {
			t.Fatalf("management mutation was not denied: %d", status)
		}
	}
	denied := func(t *testing.T, err error) {
		t.Helper()
		var failure *amqp.Error
		if !errors.As(err, &failure) || failure.Code != 403 {
			t.Fatal("AMQP operation did not return ACCESS_REFUSED")
		}
	}
	t.Run("consume-denied", func(t *testing.T) {
		_, _, err := monitorOpen(t, "MONITOR_READ_AMQP").Get("matching.requests", false)
		denied(t, err)
	})
	t.Run("configure-denied", func(t *testing.T) {
		_, err := monitorOpen(t, "MONITOR_READ_AMQP").QueueDeclare("monitor-forbidden", false, false, false, false, nil)
		denied(t, err)
	})
	t.Run("default-publish-denied", func(t *testing.T) {
		ch := monitorOpen(t, "MONITOR_READ_AMQP")
		if err := ch.Confirm(false); err != nil {
			t.Fatal(err)
		}
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		closed := ch.NotifyClose(make(chan *amqp.Error, 1))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ch.PublishWithContext(ctx, "", "matching.requests", true, false, amqp.Publishing{Body: []byte("forbidden")}); err != nil {
			denied(t, err)
			return
		}
		select {
		case err := <-closed:
			denied(t, err)
		case <-confirms:
			t.Fatal("monitor publish was accepted")
		case <-ctx.Done():
			t.Fatal("publish denial not observed")
		}
	})
	ch := monitorOpen(t, "MONITOR_APP_AMQP")
	delivery, ok, err := ch.Get("matching.requests", false)
	if err != nil || !ok || string(delivery.Body) != "ui-monitor-upgrade-fixture" {
		t.Fatal("application account lost its persistent message")
	}
	if err := delivery.Ack(false); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ch.Get("matching.requests", false); err != nil || ok {
		t.Fatal("unexpected message after denied monitor publish")
	}
}
