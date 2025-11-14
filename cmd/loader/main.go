package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"

	"wikimedia-opensearch-pipe/internal/config"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/segmentio/kafka-go"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		return
	}

	slog.Info("config loaded", "cfg", cfg)

	osClient, err := opensearchapi.NewClient(
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

	infoResp, err := osClient.Info(ctx, nil)
	if err != nil {
		slog.Error("failed to get cluster info", "error", err)
		os.Exit(1)
	}

	slog.Info("cluster info", "cluster_name", infoResp.ClusterName, "cluster_uuid", infoResp.ClusterUUID)

	var wg sync.WaitGroup
	for i := 0; i < cfg.NumConsumers; i++ {
		wg.Go(func() {
			runConsumer(i, cfg, ctx)
		})
	}
	wg.Wait()
}

func runConsumer(id int, cfg *config.Config, ctx context.Context) {
	slog.Info("starting consumer", "id", id)

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(cfg.KafkaBrokers, ","),
		Topic:   cfg.KafkaTopic,
		GroupID: "opensearch-consumer",
	})
	defer kafkaReader.Close()

	for {
		msg, err := kafkaReader.ReadMessage(ctx)
		if err != nil {
			slog.Error("failed to read message", "error", err)
			break
		}

		slog.Info("message", "partition", msg.Partition, "offset", msg.Offset, "key", msg.Key)
	}
}
