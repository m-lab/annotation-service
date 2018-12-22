package handler

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/m-lab/annotation-service/geoloader"

	"golang.org/x/net/context"

	"cloud.google.com/go/pubsub"
)

// waitForDownloaderMessages is a function that will create a new
// subscription on the "downloader-new-files" pubsub topic, listen to
// it, and process the messages that come through, triggering an
// update to the data set. It will never return. If it encounters an
// error it will halt the program.
func waitForDownloaderMessages() {
	ctx := context.Background()
	// Get a client to connect to the pubsub service
	client, err := pubsub.NewClient(ctx, os.Getenv("GCLOUD_PROJECT"))
	if err != nil {
		log.Fatal(err)
	}
	// Create a fresh subscriptions just for this instance
	sub, err := client.CreateSubscription(ctx,
		"annotator-"+strconv.FormatInt(time.Now().UnixNano(), 10),
		pubsub.SubscriptionConfig{
			Topic:       client.Topic("downloader-new-files"),
			AckDeadline: 30 * time.Second,
		})
	if err != nil {
		log.Fatal(err)
	}
	// Block forever to listen for new messages and run the refresh dataset callbacks when a new message arrives
	log.Fatal(sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
		geoloader.UpdateArchivedFilenames()
		m.Ack()
	}))
}
