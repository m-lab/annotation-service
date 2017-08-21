package main

import (
	"log"
	"net/http"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/metrics"
)

func main() {
	handler.SetupHandlers()
	metrics.SetupPrometheus()
	// TODO setup data structures here
	// TODO(JM) setup pubsub here
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
