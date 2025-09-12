package main

import (
	"encoding/json"
	"fmt"
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

type Payload struct {
	Data map[string]interface{} `json:"data"`
}

func producerHandler(writer *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wr http.ResponseWriter, req *http.Request) {
		var raw map[string]interface{}
		if err := json.NewDecoder(req.Body).Decode(&raw); err != nil {
			http.Error(wr, "Invalid JSON", http.StatusBadRequest)
			return
		}

		wrapped := Payload{Data: raw}
		valueBytes, err := json.Marshal(wrapped)
		if err != nil {
			log.Println("JSON marshal error:", err)
			return
		}

		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
			Value: valueBytes,
		}

		log.Println("Message sent to Kafka:", string(valueBytes))

		if err := writer.WriteMessages(req.Context(), msg); err != nil {
			log.Println("Kafka write error!!!:", err)
		} else {
			log.Println("Kafka write success")
		}
	})
}

func main() {
	fmt.Print("hi")
	_ = godotenv.Load("../.env")
	kafkaURL := "localhost:9094"
	kafkaTopic := "my_location_topic12"
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
