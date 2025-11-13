package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"wikimedia-opensearch-pipe/internal/config"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		return
	}

	brokers := strings.Split(cfg.KafkaBrokers, ",")

	slog.Info("config loaded", "cfg", cfg)

	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    cfg.KafkaTopic,
		Balancer: &kafka.Hash{},
	}
	defer kafkaWriter.Close()

	req, err := http.NewRequestWithContext(ctx, "GET", cfg.SseSource, nil)
	if err != nil {
		slog.Error("failed to create request", "error", err)
		os.Exit(1)
	}
	req.Header.Set("User-Agent", "wikimedia-opensearch-pipe/1.0 (https://github.com/danielwiratman/wikimedia-opensearch-pipe)")

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		slog.Error("failed to connect to Wikimedia stream", "error", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	slog.Info("connected to Wikimedia stream", "url", cfg.SseSource)

	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		line := scanner.Text()

		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		raw := strings.TrimPrefix(line, "data: ")

		var parsedJson map[string]any
		if err := json.Unmarshal([]byte(raw), &parsedJson); err != nil {
			slog.Error("failed to parse JSON", "error", err)
			continue
		}

		slog.Info("data", "wiki", parsedJson["wiki"])

		msg := kafka.Message{
			Value: []byte(raw),
		}

		if err := kafkaWriter.WriteMessages(ctx, msg); err != nil {
			slog.Error("failed to write message", "error", err)
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Error("failed to read Wikimedia stream", "error", err)
		os.Exit(1)
	}
}
