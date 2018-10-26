package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/metrics"
)

// Update the list of maxmind datasets daily
func updateMaxmindDatasets(w http.ResponseWriter, r *http.Request) {
	log.Printf("Update the list of maxmind datasets.\n")

	err := handler.UpdateFilenamelist("downloader-" + os.Getenv("GCLOUD_PROJECT"))
	if err != nil {
		log.Print(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	handler.PopulateLatestData()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func main() {
	log.Print("Beginning Setup\n")
	http.HandleFunc("/cron/update_maxmind_datasets", updateMaxmindDatasets)

	handler.UpdateFilenamelist("downloader-" + os.Getenv("GCLOUD_PROJECT"))
	handler.PopulateLatestData()

	handler.SetupHandlers()
	metrics.SetupPrometheus()

	log.Print("Listening on port 8080")
	srv := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
