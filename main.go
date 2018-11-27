package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/metrics"
)

// Update the list of maxmind datasets daily
func updateMaxmindDatasets(w http.ResponseWriter, r *http.Request) {
	log.Printf("Update the list of maxmind datasets.\n")

	err := handler.InitDatasets()
	if err != nil {
		log.Print(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	log.Print("Beginning Setup\n")
	http.HandleFunc("/cron/update_maxmind_datasets", updateMaxmindDatasets)

	handler.Init()

	metrics.SetupPrometheus()
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
