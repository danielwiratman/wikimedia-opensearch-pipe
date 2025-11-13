package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"os"

	"wikimedia-opensearch-pipe/internal/config"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		return
	}

	slog.Info("config loaded", "cfg", cfg)

	client, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
				Addresses: []string{cfg.OpensearchURL},
				Username:  cfg.OpensearchUsername,
				Password:  cfg.OpensearchPassword,
			},
		},
	)
	if err != nil {
		slog.Error("failed to create client", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	infoResp, err := client.Info(ctx, nil)
	if err != nil {
		slog.Error("failed to get cluster info", "error", err)
		os.Exit(1)
	}

	slog.Info("cluster info", "cluster_name", infoResp.ClusterName, "cluster_uuid", infoResp.ClusterUUID)
}
