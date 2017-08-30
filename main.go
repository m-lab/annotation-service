package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/metrics"
)

func main() {
	log.Print("Begining Setup\n")
	handler.PopulateLatestData()
	handler.SetupHandlers()
	metrics.SetupPrometheus()
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
