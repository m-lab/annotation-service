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

	"github.com/m-lab/annotation-service/handler/geoip"
	"github.com/m-lab/annotation-service/metrics"
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/search"
	"github.com/m-lab/etl/annotation"
)

const (
	// Maximum number of Geolite2 datasets in memory.
	MaxHistoricalGeolite2Dataset = 3

	// Maximum number of legacy datasets in memory.
	// IPv4 and IPv6 are separated for legacy datasets.
	MaxHistoricalLegacyDataset = 10

	// Maximum number of pending datasets that can be loaded at the same time.
	MaxPendingDataset = 2
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type DatasetInMemory struct {
	sync.RWMutex
	current    *parser.GeoDataset
	data       map[string]*parser.GeoDataset
	legacyData map[string]*geoip.GeoIP
}

func (d *DatasetInMemory) Init() {
	d.current = nil
	d.data = make(map[string]*parser.GeoDataset)
	d.legacyData = make(map[string]*geoip.GeoIP)
}

// This func will make the data map size to 1 and contains only the current dataset.
func (d *DatasetInMemory) SetCurrentDataset(inputData *parser.GeoDataset) {
	d.Lock()
	d.current = inputData
	d.Unlock()
}

func (d *DatasetInMemory) GetCurrentDataset() *parser.GeoDataset {
	d.RLock()
	defer d.RUnlock()
	return d.current
}

func (d *DatasetInMemory) AddDataset(filename string, inputData *parser.GeoDataset) {
	d.Lock()
	log.Println(d.data)
	if len(d.data) >= MaxHistoricalGeolite2Dataset {
		// Remove one entry
		for key, _ := range d.data {
			log.Println("remove Geolite2 dataset " + key)
			d.data[key].Free()
			delete(d.data, key)
			break
		}
	}
	d.data[filename] = inputData
	log.Printf("number of dataset in memory: %d ", len(d.data))
	d.Unlock()
}

func (d *DatasetInMemory) GetDataset(filename string) *parser.GeoDataset {
	d.RLock()
	defer d.RUnlock()
	return d.data[filename]
}

func (d *DatasetInMemory) GetLegacyDataset(filename string) *geoip.GeoIP {
	d.RLock()
	defer d.RUnlock()
	return d.legacyData[filename]
}

func (d *DatasetInMemory) AddLegacyDataset(filename string, inputData *geoip.GeoIP) {
	d.Lock()
	log.Println(d.legacyData)
	if len(d.legacyData) >= MaxHistoricalLegacyDataset {
		// Remove one entry
		for key, _ := range d.legacyData {
			log.Println("remove legacy dataset " + key)
			d.legacyData[key].Free()
			delete(d.legacyData, key)
			break
		}
	}
	d.legacyData[filename] = inputData
	log.Printf("number of legacy dataset in memory: %d ", len(d.legacyData))
	d.Unlock()
}

var (
	// This is a struct containing the latest data for the annotator to search
	// and reply with. The size of data map inside is 1.
	CurrentGeoDataset DatasetInMemory

	// The GeoLite2 datasets (except the current one) that are already in memory.
	Geolite2DatasetInMemory DatasetInMemory

	// The legacy datasets that are already in memory.
	LegacyDatasetInMemory DatasetInMemory

	// The list of dataset that was loading right now.
	// Due to memory limits, the length of PendingDataset should not exceed 2.
	PendingDataset = []string{}

	// channel to protect PendingDataset
	PendingMutex = &sync.RWMutex{}
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
func ValidateAndParse(r *http.Request) (*annotation.RequestData, error) {
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
		return &annotation.RequestData{ip, 4, time.Unix(time_milli, 0)}, nil
	}
	return &annotation.RequestData{ip, 6, time.Unix(time_milli, 0)}, nil
}

// BatchResponse is the response type for batch requests.  It is converted to
// json for HTTP requests.
type BatchResponse struct {
	Version string
	Date    time.Time
	Results map[string]*annotation.GeoData
}

// NewBatchResponse returns a new response struct.
// Caller must properly initialize the version and date strings.
// TODO - pass in the data source and use to populate the version/date.
func NewBatchResponse(size int) *BatchResponse {
	responseMap := make(map[string]*annotation.GeoData, size)
	return &BatchResponse{"", time.Time{}, responseMap}
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
		fmt.Fprintf(w, "Invalid Request!")
		return
	}

	// TODO: speed up the batch process because they will use the same dataset most of the time.
	responseMap := make(map[string]*annotation.GeoData)
	for _, data := range dataSlice {
		responseMap[data.IP+strconv.FormatInt(data.Timestamp.Unix(), encodingBase)], err = GetMetadataForSingleIP(&data)
		if err != nil {
			// stop sending more request in the same batch because w/ high chance the dataset is not ready
			fmt.Fprintf(w, "legacy dataset not loaded")
			return
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
			return nil, errors.New("invalid IP address")
		}
		ipType := 6
		if newIP.To4() != nil {
			ipType = 4
		}
		validatedData = append(validatedData, annotation.RequestData{data.IP, ipType, data.Timestamp})
	}
	return validatedData, nil
}

func UseGeoLite2Dataset(request *annotation.RequestData, dataset *parser.GeoDataset) (*annotation.GeoData, error) {
	if dataset == nil {
		// TODO: Block until the value is not nil
		return nil, errors.New("Dataset is not ready")
	}

	err := errors.New("unknown IP format")
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

func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func Deletes(a []string, x string) []string {
	for i, v := range a {
		if v == x {
			copy(a[i:], a[i+1:])
			a[len(a)-1] = ""
			a = a[:len(a)-1]
			return a
		}
	}
	return a
}

// GetMetadataForSingleIP takes a pointer to a annotation.RequestData
// struct and will use it to fetch the appropriate associated
// metadata, returning a pointer. It is gaurenteed to return a non-nil
// pointer, even if it cannot find the appropriate metadata.
func GetMetadataForSingleIP(request *annotation.RequestData) (*annotation.GeoData, error) {
	metrics.Metrics_totalLookups.Inc()
	if request.Timestamp.After(LatestDatasetDate) {
		//log.Println("Use latest dataset")
		return UseGeoLite2Dataset(request, CurrentGeoDataset.GetCurrentDataset())
	}
	// Check the timestamp of request for correct dataset.
	isIP4 := true
	if request.IPFormat == 6 {
		isIP4 = false
	}
	filename, err := SelectGeoLegacyFile(request.Timestamp, BucketName, isIP4)
	//log.Println("legacy dataset: " + filename)

	if err != nil {
		return nil, errors.New("Cannot get historical dataset")
	}
	if GeoLite2Regex.MatchString(filename) {
		if parser := Geolite2DatasetInMemory.GetDataset(filename); parser != nil {
			return UseGeoLite2Dataset(request, parser)
		} else {
			PendingMutex.Lock()
			// check whether loaded again
			if parser := Geolite2DatasetInMemory.GetDataset(filename); parser != nil {
				PendingMutex.Unlock()
				return UseGeoLite2Dataset(request, parser)
			}

			if Contains(PendingDataset, filename) {
				PendingMutex.Unlock()
				// dataset loading, just return.
				return nil, errors.New("Historical dataset is loading into memory right now " + filename)
			}
			if len(PendingDataset) >= MaxPendingDataset {
				PendingMutex.Unlock()
				return nil, errors.New("Too many pending loading right now, cannot load " + filename)
			}
			log.Println("Load new GeoLite2 dataset into memory " + filename)
			PendingDataset = append(PendingDataset, filename)
			log.Println(PendingDataset)

			parser, err := LoadGeoLite2Dataset(filename, BucketName)
			if err != nil {
				PendingMutex.Unlock()
				log.Println(err)
				return nil, errors.New("Cannot load historical dataset into memory")
			}
			log.Println("historical GeoLite2 dataset loaded " + filename)

			PendingDataset = Deletes(PendingDataset, filename)
			log.Println(PendingDataset)
			Geolite2DatasetInMemory.AddDataset(filename, parser)
			PendingMutex.Unlock()

			return UseGeoLite2Dataset(request, Geolite2DatasetInMemory.GetDataset(filename))

		}
	} else {
		if parser := LegacyDatasetInMemory.GetLegacyDataset(filename); parser != nil {
			if rec := GetRecordFromLegacyDataset(request.IP, parser, isIP4); rec != nil {
				return rec, nil
			}
			return nil, errors.New("No legacy record for the request")
		} else {
			PendingMutex.Lock()
			// check whether loaded again
			if parser := LegacyDatasetInMemory.GetLegacyDataset(filename); parser != nil {
				PendingMutex.Unlock()
				return GetRecordFromLegacyDataset(request.IP, parser, isIP4), nil
			}
			if Contains(PendingDataset, filename) {
				PendingMutex.Unlock()
				// dataset loading, just return.
				return nil, errors.New("Historical dataset is loading into memory right now " + filename)
			}
			if len(PendingDataset) >= MaxPendingDataset {
				PendingMutex.Unlock()
				return nil, errors.New("Too many pending loading right now, cannot load " + filename)
			}

			log.Println("Load new legacy dataset into memory " + filename)
			PendingDataset = append(PendingDataset, filename)

			parser, err := LoadLegacyGeoliteDataset(filename, BucketName)
			if err != nil {
				return nil, errors.New("Cannot load historical dataset into memory " + filename)
			}
			log.Println("historical legacy dataset loaded " + filename)

			PendingDataset = Deletes(PendingDataset, filename)
			LegacyDatasetInMemory.AddLegacyDataset(filename, parser)
			PendingMutex.Unlock()

			if rec := GetRecordFromLegacyDataset(request.IP, LegacyDatasetInMemory.GetLegacyDataset(filename), isIP4); rec != nil {
				return rec, nil
			}
			return nil, errors.New("No legacy record for the request")
		}
	}
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
		ASN: &annotation.IPASNData{},
	}

}
