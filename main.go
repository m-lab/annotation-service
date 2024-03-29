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

	"github.com/m-lab/annotation-service/geoloader"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/manager"
	"github.com/m-lab/go/memoryless"
	"github.com/m-lab/go/prometheusx"
)

var (
	updateInterval = flag.Duration("update_interval", time.Duration(24)*time.Hour, "Run the update dataset job with this frequency.")
	minInterval    = flag.Duration("min_interval", time.Duration(18)*time.Hour, "minimum gap between 2 runs.")
	maxInterval    = flag.Duration("max_interval", time.Duration(26)*time.Hour, "maximum gap between 2 runs.")

	maxmindDates   = flag.String("maxmind_dates", `\d{4}/\d{2}/\d{2}`, "Regex used to match Maxmind file dates.")
	routeViewDates = flag.String("routeview_dates", `\d{4}/\d{2}`, "Regex used to match RouteView file dates")
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
		fmt.Fprintf(w, "Release: %s <br>  Commit: <a href=\"https://github.com/m-lab/annotation-service/tree/%s\">%s</a><br>\n",
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

func live(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func ready(w http.ResponseWriter, r *http.Request) {
	ann, _ := manager.GetAnnotator(time.Now())
	if ann == nil {
		m := runtime.MemStats{}
		runtime.ReadMemStats(&m)
		log.Printf("Service still unavailable.  Alloc:%v MiB, TotalAlloc:%v MiB, Sys:%v MiB\n",
			m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	flag.Parse()

	geoloader.UpdateASNDatePattern(*routeViewDates)
	geoloader.UpdateGeoliteDatePattern(*maxmindDates)

	runtime.SetBlockProfileRate(1000000) // 1 sample/msec
	runtime.SetMutexProfileFraction(1000)

	log.Print("Beginning Setup\n")
	prometheusx.MustStartPrometheus(":9090")

	go memoryless.Run(ctx, manager.MustUpdateDirectory,
		memoryless.Config{Expected: *updateInterval, Min: *minInterval, Max: *maxInterval})

	http.HandleFunc("/status", Status)
	http.HandleFunc("/updateDatasets", updateMaxmindDatasets)
	http.HandleFunc("/ready", ready)
	http.HandleFunc("/live", live)

	handler.InitHandler()
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
