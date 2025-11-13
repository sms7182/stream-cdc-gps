package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
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

func getKafkaReader(kafkaURL string, kafkaTopic string) *kafka.Reader {

	brokers := []string{kafkaURL}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   kafkaTopic,

		GroupID: "my-credits-group",

		StartOffset: kafka.FirstOffset,

		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,

		ErrorLogger: log.New(log.Writer(), "KAFKA-ERROR ", log.LstdFlags),
	})

	return reader
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

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "yourStrongPassword"
	dbname   = "trafficdb"
)

var db *sql.DB

func buildDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
}
func connectDB() *sql.DB {
	psqlInfo := buildDSN()

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}

	log.Println("Successfully connected to PostgreSQL!")

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	return db
}

func main() {
	fmt.Print("hi")
	_ = godotenv.Load("../.env")
	db = connectDB()
	defer db.Close()
	kafkaURL := "kafka:9094"
	kafkaTopic := "gps-data"
	fmt.Println("kafkaurl:", kafkaURL)
	kafkaWriter := getKafkaWriter(kafkaURL, kafkaTopic)
	defer kafkaWriter.Close()

	kafkaReader := getKafkaReader(kafkaURL, "pgdemo.public.gps-data")
	defer kafkaReader.Close()
	go calculateAndModifyData(kafkaReader)

	http.HandleFunc("/location", producerHandler(kafkaWriter))
	fmt.Print("start producer-api ....!")
	log.Fatal(http.ListenAndServe(":7575", nil))
}

func calculateAndModifyData(reader *kafka.Reader) {
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Consumer read err:", err)
			continue
		}
		var payload DebeziumPayload
		err = json.Unmarshal(msg.Value, &payload)
		if err != nil {
			log.Printf("Failed to unmarshal JSON: %v", err)
			continue
		}
		func() {
			wktString := fmt.Sprintf("POINT(%f %f)", payload.After.Longitude, payload.After.Latitude)

			const sqlStatement = `
				INSERT INTO location_instances (gps_id, geo_point,latitude,longitude)
				VALUES ($1, ST_GeomFromText($2, 4326),$3,$4); 
				`

			_, err = db.Exec(
				sqlStatement,
				payload.After.ID,
				wktString,
				payload.After.Latitude,
				payload.After.Longitude,
			)
			if err != nil {
				log.Fatalf("Failed to execute insert query: %v", err)
			}
		}()
		log.Println("Consumed message:%s \n", string(msg.Value))

	}
}

type DebeziumPayload struct {
	After struct {
		ID string `json:"id"`

		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	} `json:"after"`
}
