package annotator

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
	"os"
	
	"golang.org/x/net/context" 
//	"google.golang.org/appengine"
)

var geoData []Node

func init() {
	//TODO: if moved to flex, put handlers in main()

	setupPrometheus()
	http.HandleFunc("/", handler)
	http.HandleFunc("/annotate", annotate)
	InitializeTable() 
}

func InitializeTable(){

	ctx := context.Background()

	storageReader, err := createReader("test-annotator-sandbox", "annotator-data/GeoIPCountryWhois.csv", ctx)
	if err != nil {
		return
	}
	
	geoData, err = createList(storageReader)
	if err != nil{
		os.Exit(1) 	
	}
}

func annotate(w http.ResponseWriter, r *http.Request) {
	ip, _, err := validate(w, r)
	if err != nil {
		fmt.Fprint(w, "bad validation") 
		os.Exit(1) 
	}

	//handle errors here also pass JSON out 
	lookupAndRespond(geoData, w, ip)
}

// validates request syntax
// parses request and returns parameters
func validate(w http.ResponseWriter, r *http.Request) (s string, num time.Time, err error) {
	timerStart := time.Now()
	defer metrics_requestTimes.Observe(float64(time.Since(timerStart).Nanoseconds()))

	metrics_activeRequests.Inc()
	defer metrics_activeRequests.Dec()

	query := r.URL.Query()

	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		fmt.Fprint(w, "INVALID TIME!")
		return s, num, errors.New("Invalid Time!")
	}	

	ip := query.Get("ip_addr")

	if net.ParseIP(ip) == nil {
		fmt.Fprint(w, "NOT A RECOGNIZED IP FORMAT!")
		return s, num, errors.New("Strings dont match.")
	}

	return ip, time.Unix(time_milli, 0), nil
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Place requests to /annotate with URL parameters ip_addr and since_epoch!")
}
