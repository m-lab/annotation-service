package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/m-lab/go/prometheusx"

	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/manager"
	"github.com/m-lab/go/memoryless"
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

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	runtime.SetBlockProfileRate(1000000) // 1 sample/msec
	runtime.SetMutexProfileFraction(1000)

	log.Print("Beginning Setup\n")

	// set up a daily running job for updateMaxmindDatasets()
	ctx, cancel := context.WithCancel(context.Background())
	// It is very small chance that we do not redeploy the annotation service in 3 years.
	count := 1000
	f := func() {
		if count < 0 {
			cancel()
		} else {
			count--
		}
		// Update the list of maxmind datasets daily
		manager.MustUpdateDirectory()
	}
	wt := time.Duration(24 * time.Hour)
	min := time.Duration(1 * time.Minute)
	max := time.Duration(20 * time.Minute)
	go memoryless.Run(ctx, f, memoryless.Config{Expected: wt, Min: min, Max: max})
	<-ctx.Done()

	http.HandleFunc("/status", Status)

	handler.InitHandler()
	prometheusx.MustStartPrometheus(":9090")
	log.Print("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
