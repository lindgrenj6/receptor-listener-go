package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	// run with KAFKA_HOST="host:port" to point at any kafka host
	kafka_host := os.Getenv("KAFKA_HOST")
	if kafka_host == "" {
		kafka_host = "localhost:9092"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafka_host},
		GroupID:  "consumer-group-id",
		Topic:    "platform.receptor-controller.responses",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	err := os.MkdirAll("/tmp/consumer_data", 0755)
	check(err)

	var dat map[string]interface{}
	var msgs = make([][]byte, 5)
	var count int8 = 0

	for {
		m, err := r.ReadMessage(context.Background())
		check(err)
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		err = json.Unmarshal(m.Value, &dat)
		check(err)

		if dat["message_type"].(string) == "eof" {
			msgs[count] = m.Value
			file := "/tmp/consumer_data/" + string(m.Key)

			err = ioutil.WriteFile(file, bytes.Join(msgs, []byte("\n\n")), 0644)
			check(err)
			reset(msgs, &count)
		} else {
			msgs[count] = m.Value
			count += 1
		}
	}

	r.Close()
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func reset(msgs [][]byte, count *int8) {
	for i := 0; i < len(msgs); i++ {
		msgs[i] = nil
	}

	*count = 0
}
