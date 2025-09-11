package main

import (
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

func producerHandler(writer *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wr http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			log.Fatalln(err)
		}
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
			Value: body,
		}
		log.Println("Message sent to Kafka:", string(body))

		err = writer.WriteMessages(req.Context(), msg)
		if err != nil {
			log.Println("Kafka write error!!!:", err)
		} else {
			log.Println("Kafka write success")
		}

	})
}

func main() {
	fmt.Print("hi")
	_ = godotenv.Load("../.env")
	kafkaURL := "localhost:9092"
	kafkaTopic := "my_topic"
	fmt.Println("kafkaurl:", kafkaURL)
	kafkaWriter := getKafkaWriter(kafkaURL, kafkaTopic)
	defer kafkaWriter.Close()

	http.HandleFunc("/location", producerHandler(kafkaWriter))
	fmt.Print("start producer-api ....!")
	log.Fatal(http.ListenAndServe(":7575", nil))
}
