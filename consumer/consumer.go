package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
)

func main() {
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
	i := 0
	for ; ; i++ {
		msg := <-partitionConsumer.Messages()
		if string(msg.Value) == "THE END" {
			break
		}
	}
	logger.Printf("Finished. Received %d messages.\n", i)

}
