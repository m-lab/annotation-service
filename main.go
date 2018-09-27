package main

import (
	"log"
	"net/http"
        "os"
	_ "net/http/pprof"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/handler/dataset"
	"github.com/m-lab/annotation-service/metrics"
)

// Update the list of maxmind datasets daily
func updateMaxmindDatasets(w http.ResponseWriter, r *http.Request) {
	log.Printf("Update the list of maxmind datasets.\n")

	err := dataset.UpdateFilenamelist("downloader-" + os.Getenv("GCLOUD_PROJECT"))
	if err != nil {
		log.Print(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func main() {
	log.Print("Beginning Setup\n")
	http.HandleFunc("/cron/update_maxmind_datasets", updateMaxmindDatasets)

	handler.PopulateLatestData()
	handler.SetupHandlers()
	metrics.SetupPrometheus()
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
