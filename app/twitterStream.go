package app

import (
	"fmt"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"kafkaProducer/conf"
)

func CreateTwitterClient(s *conf.AppSecrets) *twitter.Client {
	config := oauth1.NewConfig(s.TwitterConsumerKey, s.TwitterConsumerSecret)
	token := oauth1.NewToken(s.TwitterToken, s.TwitterTokenSecret)

	httpClient := config.Client(oauth1.NoContext, token)
	return twitter.NewClient(httpClient)
}

func ConfigureTwitterDemux() twitter.SwitchDemux {
	demux := twitter.NewSwitchDemux()

	demux.Tweet = func(tweet *twitter.Tweet) {
	}
	demux.DM = func(dm *twitter.DirectMessage) {
		fmt.Println(dm.SenderID)
	}
	demux.Event = func(event *twitter.Event) {
		fmt.Printf("%#v\n", event)
	}
	return demux
}
