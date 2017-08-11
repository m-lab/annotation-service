package responder

import (
	"net/http"
	"errors"
	"strconv"
	"fmt"
	"net"
	"time"
	
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/annotation-service/downloader"
	"github.com/m-lab/annotation-service/search"
)

var geoDataIPv4 []parser.Node
var geoDataIPv6 []parser.Node

var err error 

func init() {
	http.HandleFunc("/annotate", Annotate)
	metrics.SetupPrometheus() 
	geoDataIPv4, err = downloader.InitializeTable(nil,"test-annotator-sandbox","annotator-data/MaxMind/GeoIPCountryWhois.csv",4)
	if err != nil{
		errors.New("failure creating list") 
	}
	geoDataIPv6, err = downloader.InitializeTable(nil,"test-annotator-sandbox","annotator-data/MaxMind/GeoLiteCityv6.csv",6)
	if err != nil{
		errors.New("failure creating list") 
	}


}
func Annotate(w http.ResponseWriter, r *http.Request){
	IPversion, ip, _, err := validate(w, r)
	if err != nil {
		fmt.Fprintf(w,"Invalid request")
	}else{
		if IPversion == 4{
			lookupAndRespond(geoDataIPv4, w, ip)
		}
		if IPversion == 6{
			lookupAndRespond(geoDataIPv6, w, ip)
		}
	}
}

// validates request syntax
// parses request and returns parameters
func validate(w http.ResponseWriter, r *http.Request) (IPversion int, s string, num time.Time, err error) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.Metrics_requestTimes.Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)

	metrics.Metrics_activeRequests.Inc()
	defer metrics.Metrics_activeRequests.Dec()

	query := r.URL.Query()

	//PRETEND THAT THIS IS YYYYMMDD
	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		return 0, s, num, errors.New("Invalid time")
	}

	ip := query.Get("ip_addr")

	newIP := net.ParseIP(ip)
	if newIP == nil {
		return 0, s, num, errors.New("Invalid IP address.")
	}
	if newIP.To4() != nil{
		return 4, ip, time.Unix(time_milli, 0), nil
	}
	return 6, ip, time.Unix(time_milli, 0), nil
}

// searches for country codes with search func, and replies to http responder
func lookupAndRespond(list []parser.Node, w http.ResponseWriter, ip string) {
	n, err := search.SearchList(list, ip)
	if err != nil {
		fmt.Fprintf(w, "ERROR, IP ADDRESS NOT FOUND\n")
	} else {
		fmt.Fprintf(w, "[\n  {\"ip\": \"%s\", \"type\": \"STRING\"},\n  {\"country\": \"%s\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"%s\", \"type\": \"STRING\"},\n]", ip, n.CountryName, n.CountryAbrv)
	}
}
