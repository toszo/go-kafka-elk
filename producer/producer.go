package main

import (
	"context"
	"fmt"
	"time"
	"strconv"
	//"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"encoding/json"
	"math/rand"
)

type IoTData struct {
	ObjectId     string                `json:"objectId"`
	TimeStamp    time.Time             `json:"timeStamp,omitempty"`	
	Variable     string                `json:"variable,omitempty"`
	Model   	 string                `json:"model,omitempty"`
	Value        float64               `json:"value,omitempty"`
	Quality      int                   `json:"quality,omitempty"`
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
	topic := "iot-data"

	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()
	fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		t := time.Now()
		fmt.Println(t.Format("20060102150405"))
		
		for j := 0; j < 20 ; j++ {
			data := IoTData{strconv.Itoa(j), t, "variable", "my.model.co", rand.Float64(), 1}
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
		}




		//time.Sleep(1 * time.Second)
	}
}