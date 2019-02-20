package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/manager"
	"github.com/m-lab/annotation-service/metrics"
)

// Status provides a simple status page, to help understand the current running version.
// TODO(gfr) Add either a black list or a white list for the environment
// variables, so we can hide sensitive vars. https://github.com/m-lab/etl/issues/384
func Status(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>NOTE: This is just one of potentially many instances.</p>\n")
	commit := os.Getenv("COMMIT_HASH")
	if len(commit) >= 8 {
		fmt.Fprintf(w, "Release: %s <br>  Commit: <a href=\"https://github.com/m-lab/etl/tree/%s\">%s</a><br>\n",
			os.Getenv("RELEASE_TAG"), os.Getenv("COMMIT_HASH"), os.Getenv("COMMIT_HASH")[0:7])
	} else {
		fmt.Fprintf(w, "Release: %s   Commit: unknown\n", os.Getenv("RELEASE_TAG"))
	}

	//	fmt.Fprintf(w, "<p>Workers: %d / %d</p>\n", atomic.LoadInt32(&inFlight), maxInFlight)
	env := os.Environ()
	for i := range env {
		fmt.Fprintf(w, "%s</br>\n", env[i])
	}
	fmt.Fprintf(w, "</body></html>\n")
}

// Update the list of maxmind datasets daily
func updateMaxmindDatasets(w http.ResponseWriter, r *http.Request) {
	manager.MustUpdateDirectory()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	runtime.SetBlockProfileRate(1000000) // 1 sample/msec
	runtime.SetMutexProfileFraction(1000)

	log.Print("Beginning Setup\n")
	http.HandleFunc("/cron/update_maxmind_datasets", updateMaxmindDatasets)
	http.HandleFunc("/status", Status)

	handler.InitHandler()
	metrics.SetupPrometheus(9090)
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
