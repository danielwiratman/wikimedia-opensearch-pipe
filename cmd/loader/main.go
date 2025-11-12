package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	seed := "localhost:19092" // adjust for your setup

	slog.Info("connecting to Kafka broker", "broker", seed)
	conn, err := kafka.DialContext(ctx, "tcp", seed)
	if err != nil {
		slog.Error("failed to connect to broker", "broker", seed, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	// --- 1. List brokers in cluster ---
	brokers, err := conn.Brokers()
	if err != nil {
		slog.Error("failed to get broker metadata", "error", err)
		os.Exit(1)
	}

	slog.Info("cluster brokers retrieved", "count", len(brokers))
	for _, b := range brokers {
		slog.Info("broker", "id", b.ID, "addr", fmt.Sprintf("%s:%d", b.Host, b.Port))
	}

	// --- 2. Get topic metadata ---
	topic := "streaming.wikis.recentchange.json"
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		slog.Error("failed to read partitions", "topic", topic, "error", err)
		os.Exit(1)
	}

	slog.Info("partition metadata retrieved", "topic", topic, "partitions", len(partitions))
	for _, p := range partitions {
		slog.Info("partition",
			"topic", p.Topic,
			"id", p.ID,
			"leader_id", p.Leader.ID,
			"leader_host", p.Leader.Host,
			"leader_port", p.Leader.Port,
			"replicas", p.Replicas,
			"isr", p.Isr,
		)
	}

	// --- 3. Connect to a leader broker (for partition 0 as example) ---
	if len(partitions) > 0 {
		p := partitions[0]
		leaderAddr := fmt.Sprintf("%s:%d", p.Leader.Host, p.Leader.Port)
		slog.Info("connecting to leader broker",
			"topic", p.Topic,
			"partition", p.ID,
			"leader_addr", leaderAddr,
		)

		leaderConn, err := kafka.DialLeader(ctx, "tcp", leaderAddr, p.Topic, p.ID)
		if err != nil {
			slog.Error("failed to dial leader", "leader_addr", leaderAddr, "error", err)
			os.Exit(1)
		}
		defer leaderConn.Close()

		slog.Info("connected to leader", "leader_addr", leaderAddr)
	}
}
