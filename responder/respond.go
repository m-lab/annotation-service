package responder

import (
	"net/http"
	"errors"
	"strconv"
	"fmt"
	"net"
	"time"
	
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/downloader"
	"github.com/m-lab/annotation-service/search"
)

var geoDataIPv4 []parser.Node
//var geoDataIPv6 []parser.Node
var err error 

func init() {
	http.HandleFunc("/annotate", Annotate)
	
	//TODO: make this work for time stamps. 
	geoDataIPv4, err = downloader.InitializeTable(nil,"test-annotator-sandbox","annotator-data/GeoIPCountryWhois.csv",4)
	if err != nil{
		errors.New("failure creating list") 
	}
	geoDataIPv6, err = downloader.InitializeTable(nil,"test-annotator-sandbox","annotator-data/GeoLiteCityv6.csv",6)
	if err != nil{
		errors.New("failure creating list") 
	}

}
func Annotate(w http.ResponseWriter, r *http.Request){
	ip, _, err := validate(w, r)
	if err != nil {
		fmt.Fprintf(w,"Invalid request")
	}else{
		lookupAndRespond(geoData, w, ip)
	}
}
// validates request syntax
// parses request and returns parameters
func validate(w http.ResponseWriter, r *http.Request) (s string, num time.Time, err error) {
	query := r.URL.Query()

	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		return s, num, errors.New("Invalid time")
	}

	ip := query.Get("ip_addr")

	if net.ParseIP(ip) == nil {
		return s, num, errors.New("Invalid IP address.")
	}

	return ip, time.Unix(time_milli, 0), nil
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
