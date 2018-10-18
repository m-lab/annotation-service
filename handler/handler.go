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

	"github.com/m-lab/annotation-service/common"
	"github.com/m-lab/annotation-service/handler/geoip"
	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/search"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type DatasetInMemory struct {
	data  map[string]*parser.GeoDataset
	mutex *sync.RWMutex
}

func (d DatasetInMemory) Init() {
	d.date = make(map[string]*parser.GeoDataset)
	mutex = &sync.RWMutex{}
}

// This func will make the data map size to 1 and contains only the current dataset.
func (d DatasetInMemory) SetCurrentDataset(inputData *parser.GeoDataset) {
	d.mutex.Lock()
	d.date = make(map[string]*parser.GeoDataset)
	d.data["current"] = inputData
	d.mutex.Unlock()
}

func (d DatasetInMemory) AddDataset(string filename, inputData *parser.GeoDataset) {
	d.mutex.Lock()
	d.data[filename] = inputData
	d.mutex.Unlock()
}

func (d DatasetInMemory) GetDataset(string filename) *parser.GeoDataset {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return d.data[filename]
}

func (d DatasetInMemory) GetCurrentDataset() *parser.GeoDataset {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return d.data["current"]
}

var (
	// This is a struct containing the latest data for the annotator to search
	// and reply with. The size of data map inside is 1.
	CurrentGeoDataset DatasetInMemory

	// The GeoLite2 datasets (except the current one) that are already in memory.
	Geolite2DatasetInMemory DatasetInMemory

	// The legacy datasets that are already in memory.
	LegacyDatasetInMemory DatasetInMemory
)

const (
	// This is the base in which we should encode the timestamp when we
	// are creating the keys for the mapt to return for batch requests
	encodingBase = 36
)

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
		log.Println(err)
		fmt.Fprintf(w, "Invalid Request!")
		return
	}

	// TODO: speed up the batch process because they will use the same dataset most of the time.
	responseMap := make(map[string]*common.GeoData)
	for _, data := range dataSlice {
		responseMap[data.IP+strconv.FormatInt(data.Timestamp.Unix(), encodingBase)], err = GetMetadataForSingleIP(&data)
		if err != nil {
			log.Println(err)
		}
	}
	encodedResult, err := json.Marshal(responseMap)
	if err != nil {
		log.Println(w, "Unknown JSON Encoding Error")
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

func UseGeoLite2Dataset(request *common.RequestData, dataset DatasetInMemory, isCurrent bool) (*common.GeoData, error) {
	if dataset == nil {
		// TODO: Block until the value is not nil
		return nil, errors.New("Dataset is not ready")
	}
	// TODO: Figure out which table to use based on time
	err := errors.New("unknown IP format")
	var dataset *parser.GeoDataset

	var node parser.IPNode
	// TODO: Push this logic down to searchlist (after binary search is implemented)
	if request.IPFormat == 4 {
		node, err = search.SearchBinary(
			dataset.IP4Nodes, request.IP)
	} else if request.IPFormat == 6 {
		node, err = search.SearchBinary(
			dataset.IP6Nodes, request.IP)
	}

	if err != nil {
		// ErrNodeNotFound is super spammy - 10% of requests, so suppress those.
		if err != search.ErrNodeNotFound {
			log.Println(err, request.IP)
		}
		//TODO metric here
		return nil, err
	}

	return ConvertIPNodeToGeoData(node, dataset.LocationNodes), nil
}

// GetMetadataForSingleIP takes a pointer to a common.RequestData
// struct and will use it to fetch the appropriate associated
// metadata, returning a pointer. It is gaurenteed to return a non-nil
// pointer, even if it cannot find the appropriate metadata.
func GetMetadataForSingleIP(request *common.RequestData) (*common.GeoData, error) {
	metrics.Metrics_totalLookups.Inc()

	if request.Timestamp.After(LatestDatasetDate) {
		log.Println("Use latest dataset")
		return UseGeoLite2Dataset(request, CurrentGeoDataset.GetCurrentDataset())
	}
	// Check the timestamp of request for correct dataset.
	isIP4 := true
	if request.IPFormat == 6 {
		isIP4 = false
	}
	filename, err := SelectGeoLegacyFile(request.Timestamp, BucketName, isIP4)
	log.Println("legacy dataset: " + filename)

	if err != nil {
		return nil, errors.New("Cannot get historical dataset")
	}
	if GeoLite2Regex.MatchString(filename) {
		if parser := Geolite2DatasetInMemory.GetDataset(filename); parser != nil {
			log.Println("GeoLite 2 dataset already in memory")
			return UseGeoLite2Dataset(request, parser)
		} else {
			// load the new dataset into memory
			parser, err := LoadGeoLite2Dataset(filename, BucketName)
			if err != nil {
				log.Println(err)
				return nil, errors.New("Cannot load historical dataset into memory")
			}
			log.Println("Load new GeoLite 2 dataset into memory")

			Geolite2DatasetInMemory.AddDataset(filename, parser)
			return UseGeoLite2Dataset(request, parser)
		}
	} else {
		if parser, ok := LegacyDatasetInMemory[filename]; ok && parser != nil {
			log.Println("Legacy dataset already in memory")
			return GetRecordFromLegacyDataset(parser, request.IP), nil
		} else {
			parser, err := LoadLegacyGeoliteDataset(filename, BucketName)
			if err != nil {
				return nil, errors.New("Cannot load historical dataset into memory")
			}
			log.Println("Load new legacy dataset into memory")
			LegacyDatasetInMemory.AddDataset(filename, parser)
			return GetRecordFromLegacyDataset(parser, request.IP), nil
		}
	}
}

// ConvertIPNodeToGeoData takes a parser.IPNode, plus a list of
// locationNodes. It will then use that data to fill in a GeoData
// struct and return its pointer.
func ConvertIPNodeToGeoData(ipNode parser.IPNode, locationNodes []parser.LocationNode) *common.GeoData {
	locNode := parser.LocationNode{}
	if ipNode.LocationIndex >= 0 {
		locNode = locationNodes[ipNode.LocationIndex]
	}
	return &common.GeoData{
		Geo: &common.GeolocationIP{
			Continent_code: locNode.ContinentCode,
			Country_code:   locNode.CountryCode,
			Country_code3:  "", // missing from geoLite2 ?
			Country_name:   locNode.CountryName,
			Region:         locNode.RegionCode,
			Metro_code:     locNode.MetroCode,
			City:           locNode.CityName,
			Area_code:      0, // new geoLite2 does not have area code.
			Postal_code:    ipNode.PostalCode,
			Latitude:       ipNode.Latitude,
			Longitude:      ipNode.Longitude,
		},
		ASN: &common.IPASNData{},
	}

}
