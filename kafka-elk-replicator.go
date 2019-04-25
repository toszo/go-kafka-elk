package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/olivere/elastic"
	"encoding/json"
)

type IoTData struct {
	ObjectId     string                `json:"objectid"`
	TimeStamp  time.Time             `json:"timeStamp,omitempty"`
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
				"objectid":{
					"type":"keyword"
				},
				"timeStamp":{
					"type":"date"
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
	// Starting with elastic.v5, you must pass a context to execute each service
	ctx := context.Background()

	client, err := elastic.NewClient(elastic.SetURL("http://10.0.4.9:9200"))
	if err != nil {
		// Handle error
		panic(err)
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists("iot_data").Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		createIndex, err := client.CreateIndex("iot_data").BodyString(mapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	// get kafka reader using environment variables.
	//kafkaURL := os.Getenv("kafkaURL")
	kafkaURL := "10.0.4.6:9092"
	//topic := os.Getenv("topic")
	topic := "test-topic"
	//groupID := os.Getenv("groupID")
	groupID := "my-customer-group"

	reader := getKafkaReader(kafkaURL, topic, groupID)

	defer reader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}

		var dat IoTData
		merr := json.Unmarshal(m.Value, &dat)
		if merr != nil {
			log.Fatalln(err)
		}

		fmt.Printf("Obj: %s date: %s\n", dat.ObjectId, dat.TimeStamp)
				// Index a tweet (using JSON serialization)
		//data := IoTData{ObjectId: "mySuperObjId", Time: time.Now()}
		
		put1, err := client.Index().
			Index("iot_data").
			Type("iotdata").
			Id(dat.ObjectId).
			BodyJson(dat).
			Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		fmt.Printf("Indexed iotdata %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}