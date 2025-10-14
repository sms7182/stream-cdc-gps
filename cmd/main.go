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

func producerHandler(writer *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wr http.ResponseWriter, req *http.Request) {

		var data Payload
		err := json.NewDecoder(req.Body).Decode(&data)
		if err != nil {
			http.Error(wr, "Invalid JSON", http.StatusBadRequest)
			return
		}

		var test = make(map[string]interface{})
		// if err := json.Unmarshal(body, &test); err != nil {
		// 	http.Error(wr, "Invalid JSON format", http.StatusBadRequest)
		// 	log.Println("JSON validation error:", err)
		// 	return
		// }
		test["id"] = data.Location.Timestamp
		test["deviceId"] = data.DeviceID
		test["heading"] = data.Location.Coords.Heading
		test["speed"] = data.Location.Coords.Speed
		test["accuracy"] = data.Location.Coords.Accuracy
		test["altitude"] = data.Location.Coords.Altitude
		test["latitude"] = data.Location.Coords.Latitude
		test["longitude"] = data.Location.Coords.Longitude
		fields := []SchemaField{
			{Type: "string", Field: "id"},
			{Type: "string", Field: "deviceId"},
			{Type: "float", Field: "heading"},
			{Type: "float", Field: "speed"},
			{Type: "float", Field: "accuracy"},
			{Type: "float", Field: "altitude"},
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

		log.Println("Message sent to Kafka:", (data))

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
	kafkaTopic := "test1983"
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
