package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

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
		{Type: "double", Field: "heading"},
		{Type: "double", Field: "speed"},
		{Type: "double", Field: "accuracy"},
		{Type: "double", Field: "altitude"},
		{Type: "double", Field: "latitude"},
		{Type: "double", Field: "longitude"},
	},
}

func producerHandler(writer *kafka.Writer) http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		log.Printf(">>> RECEIVED: %s %s from %s", req.Method, req.URL.String(), req.RemoteAddr)
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			log.Printf("400 read body error from %s: %v", req.RemoteAddr, err)
			http.Error(wr, "failed to read body", http.StatusBadRequest)
			return
		}
		log.Printf("REQ details from %s: ct=%q body=%q", req.RemoteAddr, req.Header.Get("Content-Type"), string(bodyBytes))
		if req.Method != http.MethodPost {
			wr.Header().Set("Allow", http.MethodPost)
			http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var data Payload
		q := req.URL.Query()
		if latStr, lonStr := q.Get("lat"), q.Get("lon"); latStr != "" && lonStr != "" {
			lat, err1 := strconv.ParseFloat(latStr, 64)
			lon, err2 := strconv.ParseFloat(lonStr, 64)
			if err1 != nil || err2 != nil {
				http.Error(wr, "invalid lat or lon", http.StatusBadRequest)
				return
			}
			speed, _ := strconv.ParseFloat(q.Get("speed"), 64)
			heading, _ := strconv.ParseFloat(q.Get("bearing"), 64)
			altitude, _ := strconv.ParseFloat(q.Get("altitude"), 64)
			accuracy, _ := strconv.ParseFloat(q.Get("accuracy"), 64)
			ts := q.Get("timestamp")
			if ts == "" {
				ts = time.Now().UTC().Format(time.RFC3339)
			}
			data = Payload{
				DeviceID: q.Get("id"),
			}
			data.Location.Timestamp = ts
			data.Location.Coords.Latitude = lat
			data.Location.Coords.Longitude = lon
			data.Location.Coords.Speed = speed
			data.Location.Coords.Heading = heading
			data.Location.Coords.Altitude = altitude
			data.Location.Coords.Accuracy = accuracy
		} else if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
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
