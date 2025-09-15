package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type Location struct {
	UserId    int     `json:"userId"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func getKafkaWriter(kafkaURL string, kafkaTopic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
}

type SchemaField struct {
	Type  string `json:"type"`
	Field string `json:"field"`
}

type Schema struct {
	Type   string        `json:"type"`
	Fields []SchemaField `json:"fields"`
}

type Envelope struct {
	Schema  Schema                 `json:"schema"`
	Payload map[string]interface{} `json:"payload"`
}

func producerHandler(writer *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wr http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(wr, "Failed to read request body", http.StatusInternalServerError)
			log.Println("Read error:", err)
			return
		}

		var test map[string]interface{}
		if err := json.Unmarshal(body, &test); err != nil {
			http.Error(wr, "Invalid JSON format", http.StatusBadRequest)
			log.Println("JSON validation error:", err)
			return
		}
		fields := []SchemaField{
			{Type: "int32", Field: "id"},
			{Type: "float", Field: "latitude"},
			{Type: "float", Field: "longitude"},
		}

		schema := Schema{Type: "struct", Fields: fields}

		envelope := Envelope{Schema: schema, Payload: test}
		valueBytes, _ := json.Marshal(envelope)
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
			Value: valueBytes,
		}

		log.Println("Message sent to Kafka:", string(body))

		if err := writer.WriteMessages(req.Context(), msg); err != nil {
			log.Println("Kafka write error:", err)
		} else {
			log.Println("Kafka write success")
		}
	})
}

func main() {
	fmt.Print("hi")
	_ = godotenv.Load("../.env")
	kafkaURL := "localhost:9094"
	kafkaTopic := "my_location111"
	fmt.Println("kafkaurl:", kafkaURL)
	kafkaWriter := getKafkaWriter(kafkaURL, kafkaTopic)
	defer kafkaWriter.Close()
	// 	go func() {
	//     reader := kafka.NewReader(kafka.ReaderConfig{
	//         Brokers: []string{"localhost:9092"},
	//         Topic:   "my_topic",
	//         GroupID: "test-consumer-group",
	//     })
	//     defer reader.Close()

	//     log.Println("Test consumer started...")

	//     for {
	//         msg, err := reader.ReadMessage(context.Background())
	//         if err != nil {
	//             log.Println("Consumer read error:", err)
	//             continue
	//         }
	//         log.Printf("Consumed message: %s\n", string(msg.Value))
	//     }
	// }()

	http.HandleFunc("/location", producerHandler(kafkaWriter))
	fmt.Print("start producer-api ....!")
	log.Fatal(http.ListenAndServe(":7575", nil))
}
