package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"

	json "encoding/json"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "consumer-group-id",
		Topic:    "platform.receptor-controller.responses",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	err := os.MkdirAll("/tmp/consumer_data", 0755)
	check(err)

	var dat map[string]interface{}
	var msgs = make([][]byte, 2)

	for {
		m, err := r.ReadMessage(context.Background())
		check(err)
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		err = json.Unmarshal(m.Value, &dat)
		check(err)

		if dat["message_type"].(string) == "eof" {
			msgs[1] = m.Value
			file := "/tmp/consumer_data/" + string(m.Key)

			err = ioutil.WriteFile(file, bytes.Join(msgs, []byte("\n\n")), 0644)
			check(err)
			reset(msgs)
		} else {
			msgs[0] = m.Value
		}
	}

	r.Close()
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func reset(msgs [][]byte) {
	msgs[0] = nil
	msgs[1] = nil
}
