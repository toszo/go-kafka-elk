package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"encoding/json"
)

type IoTData struct {
	ObjectId     string                `json:"objectid"`
	TimeStamp  time.Time             `json:"timeStamp,omitempty"`
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

func main() {
	// get kafka writer using environment variables.
	// kafkaURL := os.Getenv("kafkaURL")
	// topic := os.Getenv("topic")
	kafkaURL := "10.0.4.6:9092"
	topic := "test-topic"

	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()
	fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		t := time.Now()
		fmt.Println(t.Format("20060102150405"))
		

		data := IoTData{uuid.New().String(), t}
		fmt.Println(data)
		jsonData, merr := json.Marshal(data)
		if merr != nil {
			fmt.Println(merr)
		}


		msg := kafka.Message{
			Key:   []byte(t.Format("20060102150405")),
			Value: jsonData,
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		}
		//time.Sleep(1 * time.Second)
	}
}