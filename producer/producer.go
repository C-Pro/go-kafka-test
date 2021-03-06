package main

import (
	"flag"
	"github.com/Shopify/sarama"
	"log"
	"math/rand"
	"os"
	"strings"
)

func randomString(n int) string {
	var letters = []rune("123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func produce(producer sarama.SyncProducer, c chan int, n int, s int, topic string, logger *log.Logger) {
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(randomString(s))}
	i := 0
	for ; i < n; i++ {
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			logger.Printf("FAILED to send message: %s\n", err)
		}
	}
	c <- i
}

func main() {
	msg_number := flag.Int("number", 10000, "Number of messages")
	msg_size := flag.Int("size", 1000, "Message size")
	num_threads := flag.Int("threads", 20, "Number of threads (goroutines)")
	brokers := flag.String("brokers", "localhost:9093", "Comma separated kafka brokers list")
	topic := flag.String("topic", "my-topic", "Kafka topic to send messages to")
	flag.Parse()

	logger := log.New(os.Stdout, "producer ", log.Lmicroseconds)

	//logger.Println(broker)
	cfg := sarama.NewConfig()
	//Wait for replication
	cfg.Producer.RequiredAcks = -1
	cfg.Producer.Flush.Frequency = 333
	cfg.Producer.Flush.Messages = 1000
	cfg.Producer.Flush.MaxMessages = 3000
	producer, err := sarama.NewSyncProducer(strings.Split(*brokers, ","), cfg)
	if err != nil {
		logger.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Fatalln(err)
		}
	}()

	c := make(chan int)
	logger.Println("Start")

	for i := 0; i < *num_threads; i++ {
		var chunk int
		if i == *num_threads-1 {
			chunk = *msg_number / *num_threads + (*msg_number % *num_threads)
		} else {
			chunk = *msg_number / *num_threads
		}
		go produce(producer, c, chunk, *msg_size, *topic, logger)
	}

	for i := 0; i < *num_threads; i++ {
		n := <-c
		logger.Printf("Thread%d has sent %d messages\n", i, n)
	}
	msg := &sarama.ProducerMessage{Topic: *topic, Value: sarama.StringEncoder("THE END")}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		logger.Printf("FAILED to send END message: %s\n", err)
	}

	logger.Println("Finish")

}
