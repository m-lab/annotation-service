package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/search"
	"github.com/m-lab/etl/annotation"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// A mutex to make sure that we are not reading from the dataset
// pointer while trying to update it
var currentDataMutex = &sync.RWMutex{}

// This is a pointer to a GeoDataset struct containing the absolute
// latest data for the annotator to search and reply with
var CurrentGeoDataset *parser.GeoDataset = nil

// This is the base in which we should encode the timestamp when we
// are creating the keys for the mapt to return for batch requests
const encodingBase = 36

// A function to set up any handlers that are needed, including url
// handlers and pubsub handlers
func SetupHandlers() {
	http.HandleFunc("/annotate", Annotate)
	http.HandleFunc("/batch_annotate", BatchAnnotate)
	go waitForDownloaderMessages()
}

// Annotate is a URL handler that looks up IP address and puts
// metadata out to the response encoded in json format.
func Annotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.Metrics_requestTimes.Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	metrics.Metrics_activeRequests.Inc()
	metrics.Metrics_totalRequests.Inc()
	defer metrics.Metrics_activeRequests.Dec()

	data, err := ValidateAndParse(r)
	if err != nil {
		fmt.Fprintf(w, "Invalid request")
		return
	}

	result := GetMetadataForSingleIP(data)
	encodedResult, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(w, "Unknown JSON Encoding Error")
		return
	}
	fmt.Fprint(w, string(encodedResult))
}

// ValidateAndParse takes a request and validates the URL parameters,
// verifying that it has a valid ip address and time. Then, it uses
// that to construct a RequestData struct and returns the pointer.
func ValidateAndParse(r *http.Request) (*annotation.RequestData, error) {
	query := r.URL.Query()

	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		return nil, errors.New("Invalid time")
	}

	ip := query.Get("ip_addr")

	newIP := net.ParseIP(ip)
	if newIP == nil {
		return nil, errors.New("Invalid IP address")
	}
	if newIP.To4() != nil {
		return &annotation.RequestData{ip, 4, time.Unix(time_milli, 0)}, nil
	}
	return &annotation.RequestData{ip, 6, time.Unix(time_milli, 0)}, nil
}

// BatchAnnotate is a URL handler that expects the body of the request
// to contain a JSON encoded slice of annotation.RequestDatas. It will
// look up all the ip addresses and bundle them into a map of metadata
// structs (with the keys being the ip concatenated with the base 36
// encoded timestamp) and send them back, again JSON encoded.
func BatchAnnotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.Metrics_requestTimes.Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	metrics.Metrics_activeRequests.Inc()
	metrics.Metrics_totalRequests.Inc()
	defer metrics.Metrics_activeRequests.Dec()

	dataSlice, err := BatchValidateAndParse(r.Body)
	r.Body.Close()

	if err != nil {
		fmt.Println(err)
		fmt.Fprintf(w, "Invalid Request!")
		return
	}

	responseMap := make(map[string]*annotation.GeoData)
	for _, data := range dataSlice {
		responseMap[data.IP+strconv.FormatInt(data.Timestamp.Unix(), encodingBase)] = GetMetadataForSingleIP(&data)
	}
	encodedResult, err := json.Marshal(responseMap)
	if err != nil {
		fmt.Fprintf(w, "Unknown JSON Encoding Error")
		return
	}
	fmt.Fprint(w, string(encodedResult))

}

// BatchValidateAndParse will take a reader (likely the body of a
// request) containing the JSON encoded array of
// annotation.RequestDatas. It will then validate that json and use it to
// construct a slice of annotation.RequestDatas, which it will return. If
// it encounters an error, then it will return nil and that error.
func BatchValidateAndParse(source io.Reader) ([]annotation.RequestData, error) {
	jsonBuffer, err := ioutil.ReadAll(source)
	validatedData := []annotation.RequestData{}
	if err != nil {
		return nil, err
	}
	uncheckedData := []annotation.RequestData{}

	err = json.Unmarshal(jsonBuffer, &uncheckedData)
	if err != nil {
		return nil, err
	}
	for _, data := range uncheckedData {
		newIP := net.ParseIP(data.IP)
		if newIP == nil {
			return nil, errors.New("Invalid IP address.")
		}
		ipType := 6
		if newIP.To4() != nil {
			ipType = 4
		}
		validatedData = append(validatedData, annotation.RequestData{data.IP, ipType, data.Timestamp})
	}
	return validatedData, nil
}

// GetMetadataForSingleIP takes a pointer to a annotation.RequestData
// struct and will use it to fetch the appropriate associated
// metadata, returning a pointer. It is gaurenteed to return a non-nil
// pointer, even if it cannot find the appropriate metadata.
func GetMetadataForSingleIP(request *annotation.RequestData) *annotation.GeoData {
	metrics.Metrics_totalLookups.Inc()
	if CurrentGeoDataset == nil {
		// TODO: Block until the value is not nil
		return nil
	}
	// TODO: Figure out which table to use based on time
	err := errors.New("Unknown IP Format!")
	currentDataMutex.RLock()
	// TODO(gfr) release lock sooner?
	defer currentDataMutex.RUnlock()
	var node parser.IPNode
	// TODO: Push this logic down to searchlist (after binary search is implemented)
	if request.IPFormat == 4 {
		node, err = search.SearchBinary(
			CurrentGeoDataset.IP4Nodes, request.IP)
	} else if request.IPFormat == 6 {
		node, err = search.SearchBinary(
			CurrentGeoDataset.IP6Nodes, request.IP)
	}

	if err != nil {
		log.Println(err)
		//TODO metric here
		return nil
	}

	return ConvertIPNodeToGeoData(node, CurrentGeoDataset.LocationNodes)
}

// ConvertIPNodeToGeoData takes a parser.IPNode, plus a list of
// locationNodes. It will then use that data to fill in a GeoData
// struct and return its pointer.
func ConvertIPNodeToGeoData(ipNode parser.IPNode, locationNodes []parser.LocationNode) *annotation.GeoData {
	locNode := parser.LocationNode{}
	if ipNode.LocationIndex >= 0 {
		locNode = locationNodes[ipNode.LocationIndex]
	}
	return &annotation.GeoData{
		Geo: &annotation.GeolocationIP{
			Continent_code: locNode.ContinentCode,
			Country_code:   locNode.CountryCode,
			Country_code3:  "",
			Country_name:   locNode.CountryName,
			Region:         locNode.RegionName,
			Metro_code:     locNode.MetroCode,
			City:           locNode.CityName,
			Area_code:      0, //locNode.AreaCode,
			Postal_code:    ipNode.PostalCode,
			Latitude:       ipNode.Latitude,
			Longitude:      ipNode.Longitude,
		},
		ASN: &annotation.IPASNData{},
	}

}
