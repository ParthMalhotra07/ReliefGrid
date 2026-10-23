package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

// ctl talks to the node in the same pod, so the image needs no shell or curl.
// Exit codes: 0 ok, 1 bootstrap refused, 2 usage or transport error,
// 3 CAS version conflict, 4 not the leader.
func ctl(c config, args []string) int {
	base := fmt.Sprintf("http://127.0.0.1:%d", c.httpPort)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "status":
		return printGET(ctx, base+"/status")
	case "value":
		if len(args) != 2 {
			return usage()
		}
		return printGET(ctx, base+"/value?key="+url.QueryEscape(args[1]))
	case "cas":
		if len(args) != 4 {
			return usage()
		}
		expected, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return usage()
		}
		code, body, err := postCAS(ctx, base, casCommand{Key: args[1], ExpectedVersion: expected, Value: args[3]})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		fmt.Print(body)
		switch code {
		case http.StatusOK:
			return 0
		case http.StatusConflict:
			return 3
		case http.StatusMisdirectedRequest:
			return 4
		}
		return 2
	case "load":
		return load(ctx, base, args[1:])
	case "bootstrap":
		code, msg, err := postBootstrap(ctx, base, bootstrapBody(c))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if code != http.StatusOK {
			fmt.Fprintln(os.Stderr, msg)
			return 1
		}
		fmt.Println("bootstrapped", msg)
		return 0
	}
	return usage()
}

func usage() int {
	fmt.Fprintln(os.Stderr, "usage: raft-spike ctl status | value <key> | cas <key> <expectedVersion> <value> | load [--ops N] [--concurrency C] | bootstrap")
	return 2
}

func printGET(ctx context.Context, u string) int {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(os.Stdout, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 2
	}
	return 0
}

func postCAS(ctx context.Context, base string, cmd casCommand) (int, string, error) {
	b, err := json.Marshal(cmd)
	if err != nil {
		return 0, "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/cas", bytes.NewReader(b))
	if err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), err
}

// load runs sequential CAS writes on one key per worker, so workers never conflict
// with each other; any non-200 answer counts as an error.
func load(ctx context.Context, base string, args []string) int {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	ops := fs.Int("ops", 2000, "total CAS writes")
	workers := fs.Int("concurrency", 8, "parallel workers")
	if err := fs.Parse(args); err != nil || *ops < 1 || *workers < 1 {
		return usage()
	}
	var (
		mu     sync.Mutex
		errs   int
		done   int
		wg     sync.WaitGroup
		start  = time.Now()
		perJob = *ops / *workers
	)
	for w := range *workers {
		n := perJob
		if w < *ops%*workers {
			n++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("load-%d", w)
			version := currentVersion(ctx, base, key)
			for range n {
				code, body, err := postCAS(ctx, base, casCommand{Key: key, ExpectedVersion: version, Value: strconv.FormatInt(version+1, 10)})
				mu.Lock()
				done++
				if err != nil || code != http.StatusOK {
					errs++
					mu.Unlock()
					version = currentVersion(ctx, base, key)
					continue
				}
				mu.Unlock()
				var v valueJSON
				_ = json.Unmarshal([]byte(body), &v)
				version = v.Version
			}
		}()
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	out, _ := json.Marshal(map[string]any{
		"ops": done, "errors": errs, "seconds": round1(secs), "opsPerSecond": round1(float64(done) / secs),
	})
	fmt.Println(string(out))
	if errs > 0 {
		return 2
	}
	return 0
}

func currentVersion(ctx context.Context, base, key string) int64 {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/value?key="+url.QueryEscape(key), nil)
	if err != nil {
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	var v valueJSON
	_ = json.NewDecoder(resp.Body).Decode(&v)
	return v.Version
}

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }
