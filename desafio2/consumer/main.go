package main

import (
	"fmt"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka",
		"group.id":          "desafio",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	topic := "teste"

	c.SubscribeTopics([]string{topic}, nil)

	fmt.Printf("Mensagens no tópico %s\n\n", topic)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("%s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Erro: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
