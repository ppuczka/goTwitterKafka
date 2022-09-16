package conf

import "github.com/confluentinc/confluent-kafka-go/kafka"

func CreateKafkaConfiguration(appConfig *AppConfig) kafka.ConfigMap {
	return kafka.ConfigMap{
		"bootstrap.servers": appConfig.BootstrapServers,
		"group.id":          appConfig.GroupId,
		"auto.offset.reset": "earliest",
	}
}

func ConfigureKafkaProducer(config kafka.ConfigMap) *kafka.Producer {
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		panic(err)
	}

	return producer
}
