package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
)

func main() {
	msg_number := flag.Int("number", 10000, "Exit after receiving this number of messages")
	broker_host := flag.String("host", "localhost", "Kafka broker host")
	broker_port := flag.Int("port", 9093, "Kafka broker port")
	topic := flag.String("topic", "my-topic", "Kafka topic to send messages to")
	flag.Parse()

	logger := log.New(os.Stdout, "consumer ", log.Lmicroseconds)

	broker := fmt.Sprintf("%s:%d", *broker_host, *broker_port)

	consumer, err := sarama.NewConsumer([]string{broker}, nil)
	if err != nil {
		logger.Panicln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(*topic, 0, sarama.OffsetNewest)
	if err != nil {
		logger.Panicln(err)
	}

	logger.Println("Start")
	for i := 0; i < *msg_number; i++ {
		_ = <-partitionConsumer.Messages()
	}
	logger.Println("Finish")

}
