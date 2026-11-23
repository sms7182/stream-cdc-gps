package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

// The app only publishes GPS points to Kafka. The JDBC sink connector writes
// them to Postgres table gps_data, where geo_point is a generated column
// (see migrations/001_gps_data.sql).

// getEnv returns the value of key, or def if it is unset or empty.
func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getKafkaWriter(kafkaURL string, kafkaTopic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    kafkaTopic,
		Balancer: &kafka.Hash{}, // same key (device) -> same partition, keeps per-device order
	}
}

// Kafka Connect JSON envelope (value.converter.schemas.enable=true).
type SchemaField struct {
	Type     string `json:"type"`
	Field    string `json:"field"`
	Optional bool   `json:"optional"`
}

type Schema struct {
	Type   string        `json:"type"`
	Fields []SchemaField `json:"fields"`
}

type Envelope struct {
	Schema  Schema                 `json:"schema"`
	Payload map[string]interface{} `json:"payload"`
}

type Coords struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Accuracy  float64 `json:"accuracy"`
	Speed     float64 `json:"speed"`
	Heading   float64 `json:"heading"`
	Altitude  float64 `json:"altitude"`
}

type LocationCoords struct {
	Timestamp string `json:"timestamp"`
	Coords    Coords `json:"coords"`
}

type Payload struct {
	Location LocationCoords `json:"location"`
	DeviceID string         `json:"device_id"`
}

// Field names must match the gps_data columns; types must be Kafka Connect
// schema types (float64, not float).
var locationSchema = Schema{
	Type: "struct",
	Fields: []SchemaField{
		{Type: "string", Field: "id"},
		{Type: "string", Field: "device_id"},
		{Type: "float64", Field: "heading"},
		{Type: "float64", Field: "speed"},
		{Type: "float64", Field: "accuracy"},
		{Type: "float64", Field: "altitude"},
		{Type: "float64", Field: "latitude"},
		{Type: "float64", Field: "longitude"},
	},
}

func producerHandler(writer *kafka.Writer) http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			wr.Header().Set("Allow", http.MethodPost)
			http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var data Payload
		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			http.Error(wr, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if data.DeviceID == "" {
			http.Error(wr, "device_id is required", http.StatusBadRequest)
			return
		}

		envelope := Envelope{
			Schema: locationSchema,
			Payload: map[string]interface{}{
				"id":        data.Location.Timestamp,
				"device_id": data.DeviceID,
				"heading":   data.Location.Coords.Heading,
				"speed":     data.Location.Coords.Speed,
				"accuracy":  data.Location.Coords.Accuracy,
				"altitude":  data.Location.Coords.Altitude,
				"latitude":  data.Location.Coords.Latitude,
				"longitude": data.Location.Coords.Longitude,
			},
		}
		valueBytes, err := json.Marshal(envelope)
		if err != nil {
			log.Println("Marshal error:", err)
			http.Error(wr, "internal error", http.StatusInternalServerError)
			return
		}

		msg := kafka.Message{
			Key:   []byte(data.DeviceID),
			Value: valueBytes,
		}

		if err := writer.WriteMessages(req.Context(), msg); err != nil {
			log.Println("Kafka write error:", err)
			http.Error(wr, "failed to publish location", http.StatusServiceUnavailable)
			return
		}

		log.Printf("Location from device %s sent to Kafka", data.DeviceID)
		wr.WriteHeader(http.StatusAccepted)
	}
}

func main() {
	// Local development: load .env from the repo root or the current dir.
	// In Docker the variables come from the compose file instead.
	_ = godotenv.Load(".env", "../.env")

	kafkaURL := getEnv("KAFKA_URL", "kafka:9092")
	topic := getEnv("KAFKA_PRODUCER_TOPIC", "gps-data")
	addr := ":" + getEnv("HTTP_PORT", "7575")

	log.Printf("kafka=%s topic=%s", kafkaURL, topic)

	kafkaWriter := getKafkaWriter(kafkaURL, topic)
	defer kafkaWriter.Close()

	http.HandleFunc("/location", producerHandler(kafkaWriter))
	log.Printf("producer API listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
