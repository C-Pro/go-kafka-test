package main

import (
	"flag"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
)

func main() {
	brokers := flag.String("brokers", "localhost:9093", "Comma separated kafka brokers list")
	topic := flag.String("topic", "my-topic", "Kafka topic to send messages to")
	flag.Parse()

	logger := log.New(os.Stdout, "consumer ", log.Lmicroseconds)

	consumer, err := sarama.NewConsumer(strings.Split(*brokers, ","), nil)
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
