package annotator

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
)

const appkey = "Temp Key"

var match_ip *regexp.Regexp

func init() {
	match_ip, _ = regexp.Compile(`^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|(\d|[a-fA-F]){1,4}:(\d|[a-fA-F]){1,4}:(\d|[a-fA-F]){1,4}:(\d|[a-fA-F]){1,4}:(\d|[a-fA-F]){1,4}:(\d|[a-fA-F]){1,4}:(\d|[a-fA-F]){1,4}:(\d|[a-fA-F]){1,4})$`)
	http.HandleFunc("/", handler)
	http.HandleFunc("/search_location", search_location)
	http.HandleFunc("/annotate", annotate)
}

func lookupAndRespond(w http.ResponseWriter, ip string, time_milli int64) {
	fmt.Fprintf(w, "I got ip %s and time since epoch %d.", ip, time_milli)
}

func annotate(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	ip := query.Get("ip_addr")
	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		fmt.Fprint(w, "INVALID TIME!")
		return
	}
	if !match_ip.MatchString(ip) {
		fmt.Fprint(w, "NOT A RECOGNIZED IP FORMAT!")
		return
	}
	lookupAndRespond(w, ip, time_milli)
}

func search_location(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength == 0 {
		fmt.Fprint(w, "EMPTY BODY!")
		return
	}

	body_buffer := make([]byte, r.ContentLength)
	_, err := io.ReadFull(r.Body, body_buffer)

	if err != nil {
		fmt.Fprint(w, "ERROR READING BODY")
		return
	}

	var location_request interface{}
	err = json.Unmarshal(body_buffer, &location_request)

	if err != nil {
		fmt.Fprint(w, "CANNOT PARSE REQUEST")
		return
	}
	loc_map := location_request.(map[string]interface{}) // Patch generic interface to a map of JSON key/value pairs
	fmt.Fprint(w, loc_map["IP_Addr"])
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Hello, world!")
}
