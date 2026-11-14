//go:build schedules

package workflow

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
)

// The cluster fixture and its restart probe use the app sender credential.
// Management/seam calls retain their separate operator Bearer credential.
func scheduleCoordinatorClient(ctx context.Context, url string) (*coordclient.Client, error) {
	var opts []coordclient.Option
	if path := os.Getenv("INTERNAL_AUTH_TOKEN_FILE"); path != "" {
		opts = append(opts, coordclient.WithInternalAuthFile(path))
	}
	return coordclient.New(ctx, url, opts...)
}

func scheduleHTTPClient() (*http.Client, error) {
	client := &http.Client{Timeout: time.Second}
	path := os.Getenv("INTERNAL_AUTH_TOKEN_FILE")
	if path == "" {
		return client, nil
	}
	token, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("schedule ring sender file: %w", err)
	}
	client, err = auth.NewClient(client, auth.SenderConfig{Token: string(token)})
	if err != nil {
		return nil, fmt.Errorf("schedule ring auth: %w", err)
	}
	return client, nil
}
