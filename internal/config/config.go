package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	KafkaTopic   string `env:"KAFKA_TOPIC"`
	KafkaBrokers string `env:"KAFKA_BROKERS"`
	SseSource    string `env:"SSE_SOURCE"`

	OpensearchURL      string `env:"OPENSEARCH_URL"`
	OpensearchUsername string `env:"OPENSEARCH_USERNAME"`
	OpensearchPassword string `env:"OPENSEARCH_PASSWORD"`

	NumConsumers int `env:"NUM_CONSUMERS"`
}

func Load() (*Config, error) {
	var c Config

	godotenv.Load()

	if err := env.ParseWithOptions(&c, env.Options{
		RequiredIfNoDef: true,
	}); err != nil {
		return nil, err
	}

	return &c, nil
}
