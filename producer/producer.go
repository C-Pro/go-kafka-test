package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"math/rand"
	"os"
	"time"
)

func randomString(n int) string {
	var letters = []rune("123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	msg_number := flag.Int("number", 10000, "Number of messages")
	msg_size := flag.Int("size", 1000, "Message size")
	msg_delay := flag.Int("delay", 0, "Delay between messages in ms")
	broker_host := flag.String("host", "localhost", "Kafka broker host")
	broker_port := flag.Int("port", 9093, "Kafka broker port")
	topic := flag.String("topic", "my-topic", "Kafka topic to send messages to")
	flag.Parse()

	logger := log.New(os.Stdout, "producer ", log.Lmicroseconds)

	broker := fmt.Sprintf("%s:%d", *broker_host, *broker_port)
	//logger.Println(broker)
	cfg := sarama.NewConfig()
	//Wait for replication
	cfg.Producer.RequiredAcks = -1
	producer, err := sarama.NewSyncProducer([]string{broker}, cfg)
	if err != nil {
		logger.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Fatalln(err)
		}
	}()

	msg := &sarama.ProducerMessage{Topic: *topic, Value: sarama.StringEncoder(randomString(*msg_size))}

	logger.Println("Start")
	for i := 0; i < *msg_number; i++ {
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			logger.Printf("FAILED to send message: %s\n", err)
		}
		if *msg_delay > 0 {
			time.Sleep(time.Duration(*msg_delay) * time.Millisecond)
		}
	}
	logger.Println("Finish")

}
