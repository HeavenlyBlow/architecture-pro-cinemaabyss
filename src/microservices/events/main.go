package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	brokers string
	writer  *kafka.Writer
)

func main() {
	brokers = getEnv("KAFKA_BROKERS", "kafka:9092")
	writer = &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(brokers, ",")...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	go consume()

	http.HandleFunc("/api/events/health", health)
	http.HandleFunc("/api/events/movie", movieEvent)
	http.HandleFunc("/api/events/user", userEvent)
	http.HandleFunc("/api/events/payment", paymentEvent)

	port := getEnv("PORT", "8082")
	log.Printf("events listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func produce(topic string, value []byte) (partition int, offset int64, err error) {
	msg := kafka.Message{
		Topic: topic,
		Value: value,
	}
	err = writer.WriteMessages(context.Background(), msg)
	return 0, 0, err
}

func eventResponse(partition int, offset int64, event map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	}
}

func movieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(body)
	_, offset, err := produce("movie-events", payload)
	if err != nil {
		log.Printf("produce movie-events: %v", err)
		http.Error(w, `{"error":"produce failed"}`, http.StatusInternalServerError)
		return
	}
	event := map[string]interface{}{
		"id":        fmt.Sprintf("movie-%d", time.Now().UnixNano()),
		"type":      "movie",
		"timestamp": time.Now().Format(time.RFC3339),
		"payload":   body,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(eventResponse(0, offset, event))
}

func userEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(body)
	_, offset, err := produce("user-events", payload)
	if err != nil {
		log.Printf("produce user-events: %v", err)
		http.Error(w, `{"error":"produce failed"}`, http.StatusInternalServerError)
		return
	}
	event := map[string]interface{}{
		"id":        fmt.Sprintf("user-%d", time.Now().UnixNano()),
		"type":      "user",
		"timestamp": time.Now().Format(time.RFC3339),
		"payload":   body,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(eventResponse(0, offset, event))
}

func paymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(body)
	_, offset, err := produce("payment-events", payload)
	if err != nil {
		log.Printf("produce payment-events: %v", err)
		http.Error(w, `{"error":"produce failed"}`, http.StatusInternalServerError)
		return
	}
	event := map[string]interface{}{
		"id":        fmt.Sprintf("payment-%d", time.Now().UnixNano()),
		"type":     "payment",
		"timestamp": time.Now().Format(time.RFC3339),
		"payload":   body,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(eventResponse(0, offset, event))
}

func consume() {
	topics := []string{"movie-events", "user-events", "payment-events"}
	for _, topic := range topics {
		go consumeTopic(topic)
	}
}

func consumeTopic(topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(brokers, ","),
		Topic:   topic,
		GroupID: "events-service",
	})
	defer r.Close()
	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[%s] read: %v", topic, err)
			continue
		}
		log.Printf("[%s] event: %s", topic, string(msg.Value))
	}
}
