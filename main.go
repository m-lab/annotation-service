package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/m-lab/go/memoryless"
	"github.com/m-lab/go/prometheusx"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/manager"
	"github.com/m-lab/go/memoryless"
)

var (
	updateInterval = flag.Duration("update_interval", time.Duration(24)*time.Hour, "Run the update dataset job with this frequency.")
	minInterval    = flag.Duration("min_interval", time.Duration(18)*time.Hour, "minimum gap between 2 runs.")
	maxInterval    = flag.Duration("max_interval", time.Duration(26)*time.Hour, "maximum gap between 2 runs.")
	// Create a single unified context and a cancellationMethod for said context.
	ctx, cancelCtx = context.WithCancel(context.Background())
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
	flag.Parse()

	runtime.SetBlockProfileRate(1000000) // 1 sample/msec
	runtime.SetMutexProfileFraction(1000)

	log.Print("Beginning Setup\n")
	prometheusx.MustStartPrometheus(":9090")

	go memoryless.Run(ctx, manager.MustUpdateDirectory,
		memoryless.Config{Expected: *updateInterval, Min: *minInterval, Max: *maxInterval})

	http.HandleFunc("/status", Status)
	http.HandleFunc("/updateDatasets", updateMaxmindDatasets)

	handler.InitHandler()
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
