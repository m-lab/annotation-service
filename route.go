package annotator

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"
	"errors"
	 //"golang.org/x/net/context"
	 "google.golang.org/appengine"
)

var ipRegexp *regexp.Regexp

func init() {
	ipRegexp, _ = regexp.Compile(`^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4}:(\d|[a-fA-F]){0,4})$`)
	http.HandleFunc("/", handler)
	http.HandleFunc("/annotate", annotate)
	setupPrometheus()
}


func annotate(w http.ResponseWriter, r*http.Request){
	ip,time_milli,err := validate(w,r)
	if err!=nil{
		return 
	}
	createClient(w,r,ip,time_milli) 
}

// validates request syntax
// parses request and returns parameters
func validate(w http.ResponseWriter, r *http.Request) (s string,num int64,err error) {
	timerStart := time.Now()
	defer metrics_requestTimes.Observe(float64(time.Since(timerStart).Nanoseconds()))

	metrics_activeRequests.Inc()
	defer metrics_activeRequests.Dec()

	query := r.URL.Query()

	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		fmt.Fprint(w, "INVALID TIME!")
		return s,num,errors.New("Invalid Time!") 
	}

	ip := query.Get("ip_addr")
	if !ipRegexp.MatchString(ip) {
		fmt.Fprint(w, "NOT A RECOGNIZED IP FORMAT!")
		return s,num,errors.New("Strings dont match.") 

	}

	return ip,time_milli,nil
}

// creates client to be passed to lookupAndRespond() 
// TODO: use time stamp to determine which file to open. 
func createClient(w http.ResponseWriter, r *http.Request, ip string, time_milli int64){
	
	//ctx := context.Background()
	ctx := appengine.NewContext(r) 

	storageReader,err := createReader("test-annotator-sandbox", "annotator-data/GeoIPCountryWhois.csv",ctx)
	if err != nil{
		fmt.Fprint(w, "BAD STORAGE READER\n") 
		return 
	}
	lookupAndRespond(storageReader, w, ip, time_milli)
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Place requests to /annotate with URL parameters ip_addr and since_epoch!")
}
