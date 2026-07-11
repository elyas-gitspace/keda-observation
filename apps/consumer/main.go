package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafka "github.com/segmentio/kafka-go"
)

// WikiEvent représente un event du flux Wikipedia (même format que côté producer).
type WikiEvent struct {
	Wiki      string `json:"wiki"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	User      string `json:"user"`
	Timestamp int64  `json:"timestamp"`
}

// Métriques Prometheus exposées sur /metrics.
var (
	eventsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wiki_consumer_events_total",
		Help: "Nombre total d'events Wikipedia consommés et insérés en base.",
	})
	eventsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wiki_consumer_events_failed_total",
		Help: "Nombre total d'events qui ont échoué (parsing ou insertion).",
	})
	insertDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "wiki_consumer_insert_duration_seconds",
		Help:    "Durée des insertions en base de données.",
		Buckets: prometheus.DefBuckets,
	})
)

func main() {
	brokers := getEnv("KAFKA_BROKERS", "redpanda:9092")
	topic := getEnv("KAFKA_TOPIC", "wikipedia-events")
	groupID := getEnv("KAFKA_GROUP_ID", "wikipedia-consumer-group")
	databaseURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/wikipedia?sslmode=disable")
	metricsPort := getEnv("METRICS_PORT", "9090")

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("impossible de se connecter à postgres: %v", err)
	}
	defer pool.Close()

	if err := ensureSchema(ctx, pool); err != nil {
		log.Fatalf("impossible de créer le schéma: %v", err)
	}

	// Serveur HTTP pour exposer /metrics à Prometheus.
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("serveur metrics démarré sur :%s/metrics", metricsPort)
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			log.Printf("erreur serveur metrics: %v", err)
		}
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(brokers, ","),
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Printf("consumer démarré | brokers=%s topic=%s group=%s", brokers, topic, groupID)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("erreur lecture kafka: %v", err)
			continue
		}

		if err := handleMessage(ctx, pool, msg.Value); err != nil {
			eventsFailed.Inc()
			log.Printf("erreur traitement message: %v", err)
			continue
		}

		eventsConsumed.Inc()
	}
}

func handleMessage(ctx context.Context, pool *pgxpool.Pool, payload []byte) error {
	var event WikiEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		insertDuration.Observe(time.Since(start).Seconds())
	}()

	_, err := pool.Exec(ctx, `
		INSERT INTO wiki_events (wiki, type, title, user_name, event_timestamp)
		VALUES ($1, $2, $3, $4, to_timestamp($5))
	`, event.Wiki, event.Type, event.Title, event.User, event.Timestamp)

	return err
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS wiki_events (
			id               BIGSERIAL PRIMARY KEY,
			wiki             TEXT NOT NULL,
			type             TEXT NOT NULL,
			title            TEXT NOT NULL,
			user_name        TEXT NOT NULL,
			event_timestamp  TIMESTAMPTZ NOT NULL,
			consumed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	return err
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
