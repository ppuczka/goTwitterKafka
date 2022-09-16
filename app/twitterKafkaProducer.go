package app

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dghubble/go-twitter/twitter"
	"kafkaProducer/conf"
	"sync"
	"time"
)

func ProcessTwitterStream(numWorkers int, tweets <-chan interface{}, demux twitter.SwitchDemux,
	producer *kafka.Producer, config *conf.AppConfig) <-chan string {

	var wg sync.WaitGroup
	wg.Add(config.KafkaWorkersNum)

	result := make(chan string)
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					result <- fmt.Sprintf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					result <- fmt.Sprintf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	work := func(workerId int) {
		for tweet := range tweets {
			demux.Handle(tweet)
			message := passTwitMessage(tweet, workerId, time.Now())
			conf.LOG.Sugar().Infof("[Worker ID: %d] -Delivering message to kafka", workerId)
			producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &config.Topic, Partition: kafka.PartitionAny},
				Value:          []byte(message),
			}, nil)

		}
		wg.Done()

		// Wait for message deliveries before shutting down
		producer.Flush(15 * 1000)
	}

	go func() {
		for i := 0; i < numWorkers; i++ {
			go work(i)
		}
	}()

	go func() {
		wg.Wait()
		close(result)
	}()

	return result
}

func passTwitMessage(twit interface{}, id int, currentTime time.Time) string {
	return fmt.Sprintf("[Worker ID: %d] %s: %s", id, currentTime.Format("02-01-2006 15:04:05 MST"), twit)
}
