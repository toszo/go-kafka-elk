package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"os"
	kafka "github.com/segmentio/kafka-go"
	"github.com/olivere/elastic"
	"encoding/json"
)

type IoTData struct {
	ObjectId     string                `json:"objectId"`
	TimeStamp    time.Time             `json:"timestamp,omitempty"`	
	Variable     string                `json:"variable,omitempty"`
	Model   	 string                `json:"model,omitempty"`
	Value        float64               `json:"value,omitempty"`
	Quality      int                   `json:"quality,omitempty"`
}


const mapping = `
{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"iotdata":{
			"properties":{
				"objectId":{
					"type":"keyword"
				},
				"timestamp":{
					"type":"date"
				},
				"variable":{
					"type":"keyword"
				},
				"model":{
					"type":"keyword"
				},
				"value":{
					"type":"keyword"
				},
				"quality":{
					"type":"keyword"
				}
			}
		}
	}
}`

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func main() {
	// get kafka reader using environment variables.
	kafkaURL := os.Getenv("KAFKA_URL")
	// kafkaURL := "10.0.4.6:9092"
	topic := os.Getenv("TOPIC")
	//topic := "test-topic"
	groupID := os.Getenv("GROUP_ID")
	//groupID := "my-customer-group"
	elasticUrl := os.Getenv("ELASTIC_URL")

	elkIndexName := os.Getenv("INDEX_NAME") // iot_data

	ctx := context.Background()

	client, err := elastic.NewClient(elastic.SetURL(elasticUrl)) // should be in format "http://x.y.z.zz:PORT"
	if err != nil {
		panic(err)
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(elkIndexName).Do(ctx)
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		createIndex, err := client.CreateIndex(elkIndexName).BodyString(mapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	reader := getKafkaReader(kafkaURL, topic, groupID)

	defer reader.Close()

	fmt.Printf("Start consuming Kafka: %s topic: %s, consumer group: %s, and pass to to ELK: %s, index: %s \n", kafkaURL, topic, groupID, elasticUrl, elkIndexName)
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Value %s \n", m.Value)
		var dat IoTData
		merr := json.Unmarshal(m.Value, &dat)
		if merr != nil {
			log.Fatalln(err)
		}
		
		put1, err := client.Index().
			Index(elkIndexName).
			Type("iotdata").
			Id(dat.ObjectId).
			BodyJson(dat).
			Do(ctx)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Indexed iotdata %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
	}
}