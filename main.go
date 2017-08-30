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
	//http.HandleFunc("/debug/pprof/", pprof.Index)
	//http.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	//http.HandleFunc("/debug/pprof/profile", pprof.Profile)
	//http.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	//http.HandleFunc("/debug/pprof/trace", pprof.Trace)
	handler.PopulateLatestData()
	handler.SetupHandlers()
	metrics.SetupPrometheus()
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
