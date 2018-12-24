// Package handler provides functions for handling incoming requests.
// It should only include top level code for parsing the request and assembling
// the response.
package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/annotation-service/manager"
	"github.com/m-lab/annotation-service/metrics"
)

const (
	// This is the base in which we should encode the timestamp when we
	// are creating the keys for the mapt to return for batch requests
	encodingBase = 36
)

func InitHandler() {
	// sets up any handlers that are needed, including url
	// handlers and pubsub handlers
	http.HandleFunc("/annotate", Annotate)
	http.HandleFunc("/batch_annotate", BatchAnnotate)

	// DEPRECATED
	// This code is disabled, to deal with a confusing pubsub subscription quota.
	// It is no longer needed because Ya implemented an external cron trigger.
	// This listens for pubsub messages about new downloader files, and loads them
	// when they become available.
	// go waitForDownloaderMessages()
}

// Annotate is a URL handler that looks up IP address and puts
// metadata out to the response encoded in json format.
func Annotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	tStart := time.Now()
	defer metrics.RequestTimes.Observe(float64(time.Since(tStart).Nanoseconds()))
	metrics.ActiveRequests.Inc()
	metrics.TotalRequests.Inc()
	defer metrics.ActiveRequests.Dec()

	data, err := ValidateAndParse(r)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		fmt.Fprintf(w, "Invalid request")
		return
	}

	result, err := GetMetadataForSingleIP(data)
	if err != nil {
		w.WriteHeader(http.StatusRequestTimeout)
		fmt.Fprintf(w, err.Error())
		metrics.RequestTimeHistogram.WithLabelValues("single", "error2").Observe(float64(time.Since(tStart).Nanoseconds()))
		return
	}

	encodedResult, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Unknown JSON Encoding Error")
		metrics.RequestTimeHistogram.WithLabelValues("single", "error3").Observe(float64(time.Since(tStart).Nanoseconds()))
		return
	}
	fmt.Fprint(w, string(encodedResult))
	metrics.RequestTimeHistogram.WithLabelValues("single", "success").Observe(float64(time.Since(tStart).Nanoseconds()))
}

// ValidateAndParse takes a request and validates the URL parameters,
// verifying that it has a valid ip address and time. Then, it uses
// that to construct a RequestData struct and returns the pointer.
func ValidateAndParse(r *http.Request) (*api.RequestData, error) {
	query := r.URL.Query()

	timeMilli, err := strconv.ParseInt(query.Get("since_epoch"), 10, 64)
	if err != nil {
		return nil, errors.New("invalid time")
	}

	ip := query.Get("ip_addr")

	newIP := net.ParseIP(ip)
	if newIP == nil {
		return nil, errors.New("invalid IP address")
	}
	if newIP.To4() != nil {
		return &api.RequestData{
			IP:        ip,
			IPFormat:  4,
			Timestamp: time.Unix(timeMilli, 0),
		}, nil
	}
	return &api.RequestData{
		IP:        ip,
		IPFormat:  6,
		Timestamp: time.Unix(timeMilli, 0),
	}, nil
}

// BatchResponse is the response type for batch requests.  It is converted to
// json for HTTP requests.
type BatchResponse struct {
	Version string
	Date    time.Time
	Results map[string]*api.GeoData
}

// NewBatchResponse returns a new response struct.
// Caller must properly initialize the version and date strings.
// TODO - pass in the data source and use to populate the version/date.
func NewBatchResponse(size int) *BatchResponse {
	responseMap := make(map[string]*api.GeoData, size)
	return &BatchResponse{"", time.Time{}, responseMap}
}

// TODO use error messages defined in the annotator-map PR.
var errNoAnnotator = errors.New("no Annotator found")

// AnnotateLegacy uses a single `date` to select an annotator, and uses that annotator to annotate all
// `ips`.  It uses the dates from the individual RequestData to form the keys for the result map.
// Return values include the AnnotatorDate which is the publication date of the annotation dataset.
// TODO move to annotatormanager package soon.
// DEPRECATED: This will soon be replaced with AnnotateV2()
func AnnotateLegacy(date time.Time, ips []api.RequestData) (map[string]*api.GeoData, time.Time, error) {
	responseMap := make(map[string]*api.GeoData)

	ann, err := manager.GetAnnotator(date)
	if err != nil {
		return nil, time.Time{}, err
	}
	if ann == nil {
		// stop sending more request in the same batch because w/ high chance the dataset is not ready
		return nil, time.Time{}, errNoAnnotator
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(ips))
	respLock := sync.Mutex{}
	for i := range ips {
		request := ips[i]
		metrics.TotalLookups.Inc()
		go func(req *api.RequestData) {
			annotation, err := ann.GetAnnotation(req)
			if err == nil {
				respLock.Lock()
				defer respLock.Unlock()
				dateString := strconv.FormatInt(request.Timestamp.Unix(), encodingBase)
				responseMap[request.IP+dateString] = &annotation
			} else {
				metrics.ErrorTotal.WithLabelValues(err.Error()).Inc()
			}
			wg.Done()
		}(&request)
	}
	wg.Wait()
	// TODO use annotator's actual start date.
	return responseMap, time.Time{}, nil
}

// AnnotateV2 finds an appropriate Annotator based on the requested Date, and creates a
// response with annotations for all parseable IPs.
func AnnotateV2(date time.Time, ips []string) (v2.Response, error) {
	responseMap := make(map[string]*api.GeoData, len(ips))

	ann, err := manager.GetAnnotator(date)
	if err != nil {
		return v2.Response{}, err
	}
	if ann == nil {
		// Just reject the request.  Caller should try again until successful, or different error.
		return v2.Response{}, errNoAnnotator
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(ips))
	respLock := sync.Mutex{}
	for i := range ips {
		ip := net.ParseIP(ips[i])
		if ip == nil {
			wg.Done()
			metrics.BadIPTotal.Inc()
			continue
		}
		format := 4
		if ip.To4() == nil {
			format = 6
		}
		// TODO - this is kinda hacky.  Should change the GetAnnotation api instead.
		request := api.RequestData{IP: ip.String(), IPFormat: format, Timestamp: date}
		metrics.TotalLookups.Inc()

		go func(req *api.RequestData) {
			annotation, err := ann.GetAnnotation(req)
			if err == nil {
				respLock.Lock()
				defer respLock.Unlock()
				responseMap[req.IP] = &annotation
			} else {
				metrics.ErrorTotal.WithLabelValues(err.Error()).Inc()
			}
			wg.Done()
		}(&request)
	}
	wg.Wait()
	return v2.Response{AnnotatorDate: ann.AnnotatorDate(), Annotations: responseMap}, nil
}

// BatchAnnotate is a URL handler that expects the body of the request
// to contain a JSON encoded slice of api.RequestDatas. It will
// look up all the ip addresses and bundle them into a map of metadata
// structs (with the keys being the ip concatenated with the base 36
// encoded timestamp) and send them back, again JSON encoded.
// TODO update this comment when we switch to new API.
func BatchAnnotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	timerStart := time.Now()
	defer metrics.RequestTimes.Observe(float64(time.Since(timerStart).Nanoseconds()))
	metrics.ActiveRequests.Inc()
	metrics.TotalRequests.Inc()
	defer metrics.ActiveRequests.Dec()

	jsonBuffer, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		fmt.Fprintf(w, "Invalid Request!")
		return
	}
	r.Body.Close()

	handleNewOrOld(w, jsonBuffer)
}

// TODO Leave this here for now to make review easier, rearrange later.
func handleOld(w http.ResponseWriter, jsonBuffer []byte) {
	tStart := time.Now()
	dataSlice, err := BatchValidateAndParse(jsonBuffer)
	if err != nil {
		// TODO Add metric
		fmt.Fprintf(w, "Invalid Request!")
		metrics.RequestTimeHistogram.WithLabelValues("old", "invalid").Observe(float64(time.Since(tStart).Nanoseconds()))
		return
	}

	var responseMap map[string]*api.GeoData

	// For now, use the date of the first item.  In future the items will not have individual timestamps.
	if len(dataSlice) > 0 {
		// For old request format, we use the date of the first RequestData
		date := dataSlice[0].Timestamp
		responseMap, _, err = AnnotateLegacy(date, dataSlice)
		if err != nil {
			fmt.Fprintf(w, err.Error())
			metrics.RequestTimeHistogram.WithLabelValues("old", err.Error()).Observe(float64(time.Since(tStart).Nanoseconds()))
			return
		}
	} else {
		responseMap = make(map[string]*api.GeoData)
	}

	encodedResult, err := json.Marshal(responseMap)
	if err != nil {
		// TODO Add metric
		fmt.Fprintf(w, "Unknown JSON Encoding Error")
		metrics.RequestTimeHistogram.WithLabelValues("old", err.Error()).Observe(float64(time.Since(tStart).Nanoseconds()))
		return
	}
	fmt.Fprint(w, string(encodedResult))
	if len(dataSlice) > 50 {
		metrics.RequestTimeHistogram.WithLabelValues("old", "huge").Observe(float64(time.Since(tStart).Nanoseconds()))
	} else if len(dataSlice) > 10 {
		metrics.RequestTimeHistogram.WithLabelValues("old", "large").Observe(float64(time.Since(tStart).Nanoseconds()))
	} else {
		metrics.RequestTimeHistogram.WithLabelValues("old", "small").Observe(float64(time.Since(tStart).Nanoseconds()))
	}
}

func handleV2(w http.ResponseWriter, jsonBuffer []byte) {
	tStart := time.Now()
	request := v2.Request{}

	err := json.Unmarshal(jsonBuffer, &request)
	if err != nil {
		// TODO Add metric
		metrics.RequestTimeHistogram.WithLabelValues("v2", err.Error()).Observe(float64(time.Since(tStart).Nanoseconds()))
		fmt.Fprintf(w, "Unable to parse V2.0 request %s", string(jsonBuffer))
		return
	}

	// No need to validate IP addresses, as they are net.IP
	response := v2.Response{}

	// For now, use the date of the first item.  In future the items will not have individual timestamps.
	if len(request.IPs) > 0 {
		// For old request format, we use the date of the first RequestData
		response, err = AnnotateV2(request.Date, request.IPs)
		if err != nil {
			metrics.RequestTimeHistogram.WithLabelValues("v2", err.Error()).Observe(float64(time.Since(tStart).Nanoseconds()))
			fmt.Fprintf(w, err.Error())
			return
		}
	}

	encodedResult, err := json.Marshal(response)
	if err != nil {
		// TODO Add metric
		metrics.RequestTimeHistogram.WithLabelValues("v2", err.Error()).Observe(float64(time.Since(tStart).Nanoseconds()))
		fmt.Fprintf(w, "Unknown JSON Encoding Error")
		return
	}
	fmt.Fprint(w, string(encodedResult))
	if len(request.IPs) > 50 {
		metrics.RequestTimeHistogram.WithLabelValues("v2", "huge").Observe(float64(time.Since(tStart).Nanoseconds()))
	} else if len(request.IPs) > 10 {
		metrics.RequestTimeHistogram.WithLabelValues("v2", "large").Observe(float64(time.Since(tStart).Nanoseconds()))
	} else {
		metrics.RequestTimeHistogram.WithLabelValues("v2", "small").Observe(float64(time.Since(tStart).Nanoseconds()))
	}
}

func handleNewOrOld(w http.ResponseWriter, jsonBuffer []byte) {
	// Check API version of the request
	wrapper := api.RequestWrapper{}
	err := json.Unmarshal(jsonBuffer, &wrapper)
	if err != nil {
		handleOld(w, jsonBuffer)
	} else {
		switch wrapper.RequestType {
		case v2.RequestTag:
			handleV2(w, jsonBuffer)
		default:
			// TODO Add metric
			fmt.Fprintf(w, "Unknown API version %s", wrapper.RequestType)
		}
	}
}

// BatchValidateAndParse will take a reader (likely the body of a
// request) containing the JSON encoded array of
// api.RequestDatas. It will then validate that json and use it to
// construct a slice of api.RequestDatas, which it will return. If
// it encounters an error, then it will return nil and that error.
func BatchValidateAndParse(jsonBuffer []byte) ([]api.RequestData, error) {
	uncheckedData := []api.RequestData{}

	err := json.Unmarshal(jsonBuffer, &uncheckedData)
	if err != nil {
		return nil, err
	}
	validatedData := make([]api.RequestData, 0, len(uncheckedData))
	for _, data := range uncheckedData {
		newIP := net.ParseIP(data.IP)
		if newIP == nil {
			// TODO - shouldn't bail out because of a single error.
			return nil, errors.New("invalid IP address")
		}
		ipType := 6
		if newIP.To4() != nil {
			ipType = 4
		}
		validatedData = append(validatedData, api.RequestData{IP: data.IP, IPFormat: ipType, Timestamp: data.Timestamp})
	}
	return validatedData, nil
}

// GetMetadataForSingleIP takes a pointer to a api.RequestData
// struct and will use it to fetch the appropriate associated
// metadata, returning a GeoData.
// pointer, even if it cannot find the appropriate metadata.
func GetMetadataForSingleIP(request *api.RequestData) (api.GeoData, error) {
	metrics.TotalLookups.Inc()
	ann, err := manager.GetAnnotator(request.Timestamp)
	if err != nil {
		return api.GeoData{}, err
	}
	if ann == nil {
		log.Println("This shouldn't happen")
		return api.GeoData{}, manager.ErrNilDataset
	}

	return ann.GetAnnotation(request)
}
