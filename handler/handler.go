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
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/common"
	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/search"
)

var (
	// A mutex to make sure that we are not reading from the dataset
	// pointer while trying to update it
	currentDataMutex = &sync.RWMutex{}

	// This is a pointer to a GeoDataset struct containing the absolute
	// latest data for the annotator to search and reply with
	CurrentGeoDataset *parser.GeoDataset = nil
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

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

	result, err := GetMetadataForSingleIP(data)
	if err != nil {
		fmt.Fprintf(w, "Cannot get meta data")
		return
	}

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
func ValidateAndParse(r *http.Request) (*common.RequestData, error) {
	query := r.URL.Query()

	time_milli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		return nil, errors.New("invalid time")
	}

	ip := query.Get("ip_addr")

	newIP := net.ParseIP(ip)
	if newIP == nil {
		return nil, errors.New("invalid IP address")
	}
	if newIP.To4() != nil {
		return &common.RequestData{ip, 4, time.Unix(time_milli, 0)}, nil
	}
	return &common.RequestData{ip, 6, time.Unix(time_milli, 0)}, nil
}

// BatchResponse is the response type for batch requests.  It is converted to
// json for HTTP requests.
type BatchResponse struct {
	Version string
	Date    time.Time
	Results map[string]*common.GeoData
}

// NewBatchResponse returns a new response struct.
// Caller must properly initialize the version and date strings.
// TODO - pass in the data source and use to populate the version/date.
func NewBatchResponse(size int) *BatchResponse {
	responseMap := make(map[string]*common.GeoData, size)
	return &BatchResponse{"", time.Time{}, responseMap}
}

// BatchAnnotate is a URL handler that expects the body of the request
// to contain a JSON encoded slice of common.RequestDatas. It will
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

	responseMap := make(map[string]*common.GeoData)
	for _, data := range dataSlice {
		responseMap[data.IP+strconv.FormatInt(data.Timestamp.Unix(), encodingBase)], err = GetMetadataForSingleIP(&data)
		if err != nil {
			// stop sending more request in the same batch because w/ high chance the dataset is not ready
			fmt.Fprintf(w, "Batch Request Error")
			return
		}
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
// common.RequestDatas. It will then validate that json and use it to
// construct a slice of common.RequestDatas, which it will return. If
// it encounters an error, then it will return nil and that error.
func BatchValidateAndParse(source io.Reader) ([]common.RequestData, error) {
	jsonBuffer, err := ioutil.ReadAll(source)
	validatedData := []common.RequestData{}
	if err != nil {
		return nil, err
	}
	uncheckedData := []common.RequestData{}

	err = json.Unmarshal(jsonBuffer, &uncheckedData)
	if err != nil {
		return nil, err
	}
	for _, data := range uncheckedData {
		newIP := net.ParseIP(data.IP)
		if newIP == nil {
			return nil, errors.New("invalid IP address")
		}
		ipType := 6
		if newIP.To4() != nil {
			ipType = 4
		}
		validatedData = append(validatedData, common.RequestData{data.IP, ipType, data.Timestamp})
	}
	return validatedData, nil
}

// GetMetadataForSingleIP takes a pointer to a common.RequestData
// struct and will use it to fetch the appropriate associated
// metadata, returning a pointer. It is gaurenteed to return a non-nil
// pointer, even if it cannot find the appropriate metadata.
func GetMetadataForSingleIP(request *common.RequestData) (*common.GeoData, error) {
	metrics.Metrics_totalLookups.Inc()

	return UseGeoLite2Dataset(request, CurrentGeoDataset)
}

// ExtractDateFromFilename return the date for a filename like
// gs://downloader-mlab-oti/Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz
func ExtractDateFromFilename(filename string) (time.Time, error) {
	re := regexp.MustCompile(`[0-9]{8}T`)
	filedate := re.FindAllString(filename, -1)
	if len(filedate) != 1 {
		return time.Time{}, errors.New("cannot extract date from input filename")
	}
	return time.Parse(time.RFC3339, filedate[0][0:4]+"-"+filedate[0][4:6]+"-"+filedate[0][6:8]+"T00:00:00Z")
}
