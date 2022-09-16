package main

import (
	"github.com/dghubble/go-twitter/twitter"
	"kafkaProducer/app"
	"kafkaProducer/conf"
)

func main() {

	defer conf.LOG.Sync()

	conf.LOG.Sugar().Info("Loading configuration...")
	appConfig, err := conf.LoadConfiguration("http://localhost:8888/application/dev", "http://51.103.210.207:8200")
	if err != nil {
		conf.LOG.Sugar().Error("error while loading configuration %v", err)
		return
	}
	conf.LOG.Sugar().Info("Successfully loaded configuration")

	conf.LOG.Sugar().Info("Configuring Kafka producer...")
	kafkaConfig := conf.CreateKafkaConfiguration(&appConfig)
	kafkaProducer := conf.ConfigureKafkaProducer(kafkaConfig)

	conf.LOG.Sugar().Info("Configuring Twitter client...")
	client := app.CreateTwitterClient(&appConfig.AppSecrets)
	demux := app.ConfigureTwitterDemux()

	conf.LOG.Sugar().Info("Starting Twitter Stream...")
	filterParams := &twitter.StreamFilterParams{
		Track:         []string{"IT"},
		StallWarnings: twitter.Bool(true),
	}

	stream, err := client.Streams.Filter(filterParams)
	if err != nil {
		conf.LOG.Sugar().Fatal(err)
	}

	processed := app.ProcessTwitterStream(4, stream.Messages, demux, kafkaProducer, &appConfig)
	for m := range processed {
		conf.LOG.Sugar().Info(m)
	}

}
