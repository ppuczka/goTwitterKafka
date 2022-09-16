package app

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkaProducer/conf"
	"math/rand"
	"time"
)

func kafkaWorker(id int, jobs <-chan []string, results chan<- string, producer *kafka.Producer, topic string) {
	for j := range jobs {
		message := prepareMessage(j, time.Now(), "created")
		conf.LOG.Sugar().Infof("[worker %d]: message [%s] cerated\n", id, message)
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)

		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					results <- fmt.Sprintf("[worker %d]: Delivery failed: %v\n", id, ev.TopicPartition)
				} else {
					results <- fmt.Sprintf("[worker %d]: Delivered message to %v\n", id, ev.TopicPartition)
				}
			}
		}

	}
}

func prepareMessage(words []string, currentTime time.Time, status string) string {
	seed := len(words)
	return fmt.Sprintf("%s: %s %s %s [%s]", currentTime.Format("02-01-2006 15:04:05 MST"),
		words[rand.Intn(seed)], words[rand.Intn(seed)], words[rand.Intn(seed)], status)
}

func produceMockKafkaMessages(producer *kafka.Producer, numJobs, numWorkers int) {

	topic := "kafkaTest"
	words := []string{"Lorem",
		"ipsum",
		"dolor",
		"sit",
		"amet",
		"consectetur",
		"adipiscing",
		"elit",
		"sed",
		"do",
		"eiusmod",
		"tempor",
		"incididunt",
		"ut",
		"labore",
		"et",
		"dolore",
		"magna",
		"aliqua",
		"Ut",
		"enim",
		"ad",
		"minim",
		"veniam",
		"quis",
		"nostrud",
		"exercitation",
		"ullamco",
	}

	wordsChan := make(chan []string, numJobs)
	resultsChan := make(chan string, numJobs)

	for true {
		for w := 1; w <= numWorkers; w++ {
			go kafkaWorker(w, wordsChan, resultsChan, producer, topic)
		}

		for j := 1; j <= numJobs; j++ {
			wordsChan <- words
		}

		close(wordsChan)
		for m := 1; m <= numJobs; m++ {
			fmt.Println(<-resultsChan)
		}
		close(resultsChan)
		time.Sleep(1 * time.Second)
	}

	//Wait for message deliveries before shutting down
	//producer.Flush(15 * 1000)

}
