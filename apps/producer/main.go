package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// WikiEvent représente un événement du flux Wikipedia (on ne garde que ce qui nous intéresse).
type WikiEvent struct {
	Wiki      string `json:"wiki"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	User      string `json:"user"`
	Timestamp int64  `json:"timestamp"`
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "redpanda:9092")
	topic := getEnv("KAFKA_TOPIC", "wikipedia-events")
	streamURL := getEnv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
	wikiFilter := getEnvAllowEmpty("WIKI_FILTER", "frwiki")
	sampleRate := getEnvInt("SAMPLE_RATE", 10)    // on garde 1 event sur N

	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(brokers, ",")...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 500 * time.Millisecond,
	}
	defer writer.Close()

	log.Printf("producer démarré | brokers=%s topic=%s filter=%q sample=1/%d", brokers, topic, wikiFilter, sampleRate)

	counter := 0
	for {
		if err := consumeStream(streamURL, wikiFilter, sampleRate, &counter, writer); err != nil {
			log.Printf("stream erreur: %v, reconnexion dans 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

// consumeStream se connecte au flux SSE et publie les events filtrés dans Kafka.
// Format SSE Wikipedia: lignes "data: {...json...}" séparées par des lignes vides.
func consumeStream(url, wikiFilter string, sampleRate int, counter *int, writer *kafka.Writer) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", "devops-demo-producer/1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}

		var event WikiEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			continue // ligne pas exploitable, on ignore
		}

		if wikiFilter != "" && event.Wiki != wikiFilter {
			continue // pas la bonne langue
		}

		*counter++
		if *counter%sampleRate != 0 {
			continue // throttle: on saute cet event
		}

		if err := publish(writer, payload); err != nil {
			log.Printf("erreur publish kafka: %v", err)
		}
	}
	return scanner.Err()
}

func publish(writer *kafka.Writer, payload string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return writer.WriteMessages(ctx, kafka.Message{
		Value: []byte(payload),
		Time:  time.Now(),
	})
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getEnvAllowEmpty comme getEnv, mais si la variable est explicitement
// définie (même vide), on la garde telle quelle plutôt que d'utiliser
// le fallback. Sert pour WIKI_FILTER: "" (= pas de filtre du tout).
func getEnvAllowEmpty(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}
