package handler

import (
	"log"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/context"

	"cloud.google.com/go/pubsub"
)

func waitForDownloaderMessages() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, os.Getenv("GCLOUD_PROJECT"))
	if err != nil {
		log.Fatal(err)
	}
	sub, err := client.CreateSubscription(ctx,
		"annotator-"+strconv.FormatInt(time.Now().UnixNano(), 10),
		pubsub.SubscriptionConfig{
			Topic:       client.Topic("downloader-new-files"),
			AckDeadline: 30 * time.Second,
		})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
		// TODO(JM) Reload the tables here with the same setup
		// method that will eventually be used in main
		log.Println(string(m.Data))
		m.Ack()
	}))

}
