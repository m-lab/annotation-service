// Package handler provides functions for handling incoming requests.
// It should only include top level code for parsing the request and assembling
// the response.
package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/go/logx"

	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/annotation-service/geoloader"
	"github.com/m-lab/annotation-service/manager"
	"github.com/m-lab/annotation-service/metrics"
)

const (
	// This is the base in which we should encode the timestamp when we
	// are creating the keys for the mapt to return for batch requests
	encodingBase = 36
)

// InitHandler sets up the annotator directory and registers annotation service
// HTTP handlers.
func InitHandler() {
	manager.MustUpdateDirectory()

	// sets up any handlers that are needed
	http.HandleFunc("/annotate", Annotate)
	http.HandleFunc("/batch_annotate", BatchAnnotate)
}

// Annotate is a URL handler that looks up IP address and puts
// metadata out to the response encoded in json format.
func Annotate(w http.ResponseWriter, r *http.Request) {
	// Setup timers and counters for prometheus metrics.
	tStart := time.Now()
	defer func(t time.Time) {
		metrics.RequestTimes.Observe(float64(time.Since(t).Nanoseconds()))
	}(tStart)
	metrics.ActiveRequests.Inc()
	metrics.TotalRequests.Inc()
	defer metrics.ActiveRequests.Dec()

	data, err := ValidateAndParse(r)
	if checkError(err, w, "", 1, "single", tStart) {
		return
	}

	result, err := GetMetadataForSingleIP(data)
	if checkError(err, w, "", 1, "single", tStart) {
		return
	}

	trackMissingResponses(&result)

	// Set Missing=true for empty results.
	if result.Geo == nil {
		result.Geo = &api.GeolocationIP{
			Missing: true,
		}
	}
	if result.Network == nil {
		result.Network = &api.ASData{
			Missing: true,
		}
	}

	encodedResult, err := json.Marshal(result)
	if checkError(err, w, "", 1, "single", tStart) {
		return
	}

	fmt.Fprint(w, string(encodedResult))
	metrics.RequestTimeHistogramUsec.WithLabelValues("unknown", "single", "success").Observe(float64(time.Since(tStart).Nanoseconds()) / 1000)
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

	for i := range ips {
		request := ips[i]
		metrics.TotalLookups.Inc()
		data := api.GeoData{}
		requestIP := request.IP
		if strings.HasPrefix(request.IP, "2002:") {
			requestIP = Ip6to4(request.IP)
		}
		err := ann.Annotate(requestIP, &data)
		if err != nil {
			// TODO need better error handling.
			continue
		}
		// This requires that the caller should ignore the dateString.
		// TODO - the unit tests do not catch this problem, so maybe it isn't a problem.
		dateString := strconv.FormatInt(request.Timestamp.Unix(), encodingBase)
		responseMap[request.IP+dateString] = &data
	}
	// TODO use annotator's actual start date.
	return responseMap, time.Time{}, nil
}

var v2errorLogger = logx.NewLogEvery(nil, time.Second)

// Ip6to4 converts "2002:" ipv6 address back to ipv4.
func Ip6to4(ipv6 string) string {
	ipnet := &net.IPNet{
		Mask: net.CIDRMask(16, 128),
		IP:   net.ParseIP("2002::"),
	}
	ip := net.ParseIP(ipv6)
	if ip == nil || !ipnet.Contains(ip) {
		return ""
	}

	return fmt.Sprintf("%d.%d.%d.%d", ip[2], ip[3], ip[4], ip[5])
}

// AnnotateV2 finds an appropriate Annotator based on the requested Date, and creates a
// response with annotations for all parseable IPs.
func AnnotateV2(date time.Time, ips []string, reqInfo string) (v2.Response, error) {
	responseMap := make(map[string]*api.GeoData, len(ips))

	ann, err := manager.GetAnnotator(date)
	if err != nil {
		return v2.Response{}, err
	}
	if ann == nil {
		// Just reject the request.  Caller should try again until successful, or different error.
		return v2.Response{}, errNoAnnotator
	}

	for i := range ips {
		metrics.TotalLookups.Inc()

		annotation := api.GeoData{}
		// special handling of "2002:" ip address
		requestIP := ips[i]
		if strings.HasPrefix(ips[i], "2002:") {
			requestIP = Ip6to4(ips[i])
		}
		err := ann.Annotate(requestIP, &annotation)
		if err != nil {
			switch err.Error {
			// TODO - enumerate interesting error types here...
			// Consider testing for an error subtype, rather than enumerating every error.
			default:
				// This collapses all other error types into a single error, to avoid excessive
				// time serices if there are variable error strings.
				metrics.ErrorTotal.WithLabelValues("Annotate Error").Inc()

				// We are trying to debug error propagation.  So logging errors here to help with that.
				v2errorLogger.Println(err)
			}
			continue
		}
		if annotation.Geo == nil {
			annotation.Geo = &api.GeolocationIP{
				Missing: true,
			}
		}
		if annotation.Network == nil {
			annotation.Network = &api.ASData{
				Missing: true,
			}
		}
		responseMap[ips[i]] = &annotation
	}
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
	tStart := time.Now()
	defer func(t time.Time) {
		metrics.RequestTimes.Observe(float64(time.Since(t).Nanoseconds()))
	}(tStart)
	metrics.ActiveRequests.Inc()
	metrics.TotalRequests.Inc()
	defer metrics.ActiveRequests.Dec()

	jsonBuffer, err := ioutil.ReadAll(r.Body)
	if checkError(err, w, "batch-error", 0, "", tStart) {
		return
	}
	r.Body.Close()

	handleNewOrOld(w, tStart, jsonBuffer)
}

func latencyStats(source string, label string, count int, tStart time.Time) {
	switch {
	case count >= 400:
		metrics.RequestTimeHistogramUsec.WithLabelValues(source, label, "400+").Observe(float64(time.Since(tStart).Nanoseconds()) / 1000)
	case count >= 100:
		metrics.RequestTimeHistogramUsec.WithLabelValues(source, label, "100+").Observe(float64(time.Since(tStart).Nanoseconds()) / 1000)
	case count >= 20:
		metrics.RequestTimeHistogramUsec.WithLabelValues(source, label, "20+").Observe(float64(time.Since(tStart).Nanoseconds()) / 1000)
	case count >= 5:
		metrics.RequestTimeHistogramUsec.WithLabelValues(source, label, "5+").Observe(float64(time.Since(tStart).Nanoseconds()) / 1000)
	default:
		metrics.RequestTimeHistogramUsec.WithLabelValues(source, label, "<5").Observe(float64(time.Since(tStart).Nanoseconds()) / 1000)
	}
}

// TODO - is this now obsolete?
func checkError(err error, w http.ResponseWriter, reqInfo string, ipCount int, label string, tStart time.Time) bool {
	if err != nil {
		switch err {
		default:
			// If it isn't loading, client should probably give up instead of retrying.
			w.WriteHeader(http.StatusInternalServerError)
			metrics.RequestTimeHistogramUsec.WithLabelValues(reqInfo, label, err.Error()).Observe(float64(time.Since(tStart).Nanoseconds()) / 1000)
		}
		fmt.Fprintf(w, err.Error())
		return true
	}
	return false
}

// TODO Leave this here for now to make review easier, rearrange later.
func handleOld(w http.ResponseWriter, tStart time.Time, jsonBuffer []byte) {
	dataSlice, err := BatchValidateAndParse(jsonBuffer)
	if checkError(err, w, "old", 0, "", tStart) {
		return
	}

	var responseMap map[string]*api.GeoData

	// For now, use the date of the first item.  In future the items will not have individual timestamps.
	if len(dataSlice) > 0 {
		// For old request format, we use the date of the first RequestData
		date := dataSlice[0].Timestamp
		responseMap, _, err = AnnotateLegacy(date, dataSlice)
		if checkError(err, w, "old", len(dataSlice), "", tStart) {
			return
		}
	} else {
		responseMap = make(map[string]*api.GeoData)
	}
	for _, anno := range responseMap {
		trackMissingResponses(anno)
		// Set Missing=true for empty results.
		if anno.Geo == nil {
			anno.Geo = &api.GeolocationIP{
				Missing: true,
			}
		}
		if anno.Network == nil {
			anno.Network = &api.ASData{
				Missing: true,
			}
		}
	}
	encodedResult, err := json.Marshal(responseMap)
	if checkError(err, w, "old", len(dataSlice), "", tStart) {
		return
	}
	fmt.Fprint(w, string(encodedResult))

	if len(dataSlice) == 0 {
		// Don't know if this was legacy or geolite2, so just label it "old"
		latencyStats("old", "unknown", len(dataSlice), tStart)
	} else if geoloader.IsLegacy(dataSlice[0].Timestamp) {
		// Label this old (api) and legacy (dataset)
		latencyStats("old", "legacy", len(dataSlice), tStart)
	} else {
		// Label this old (api) and geolite2 (dataset)
		latencyStats("old", "geolite2", len(dataSlice), tStart)
	}
}

func trackMissingResponses(anno *api.GeoData) {
	if anno == nil {
		metrics.ResponseMissingAnnotation.WithLabelValues("nil-response").Inc()
		return
	}

	netOk := anno.Network != nil && len(anno.Network.Systems) > 0 && len(anno.Network.Systems[0].ASNs) > 0 && anno.Network.Systems[0].ASNs[0] != 0
	geoOk := anno.Geo != nil && anno.Geo.Latitude != 0 && anno.Geo.Longitude != 0

	if netOk && geoOk {
		return
	}
	if netOk {
		if anno.Geo == nil {
			metrics.ResponseMissingAnnotation.WithLabelValues("nil-geo").Inc()
		} else {
			metrics.ResponseMissingAnnotation.WithLabelValues("empty-geo").Inc()
		}
	} else if geoOk {
		if anno.Network == nil {
			metrics.ResponseMissingAnnotation.WithLabelValues("nil-asn").Inc()
		} else {
			metrics.ResponseMissingAnnotation.WithLabelValues("empty-asn").Inc()
		}
	} else {
		metrics.ResponseMissingAnnotation.WithLabelValues("both").Inc()
	}
}

func handleV2(w http.ResponseWriter, tStart time.Time, jsonBuffer []byte) {
	request := v2.Request{}

	err := json.Unmarshal(jsonBuffer, &request)
	if checkError(err, w, request.RequestInfo, 0, "v2", tStart) {
		return
	}

	// No need to validate IP addresses, as they are net.IP
	response := v2.Response{}

	if len(request.IPs) > 0 {
		requestIPs := make([]string, len(request.IPs))
		for i := range request.IPs {
			requestIPs[i] = request.IPs[i]
			if strings.HasPrefix(request.IPs[i], "2002:") {
				requestIPs[i] = Ip6to4(request.IPs[i])
			}
		}
		response, err = AnnotateV2(request.Date, requestIPs, request.RequestInfo)
		if checkError(err, w, request.RequestInfo, len(request.IPs), "v2", tStart) {
			return
		}
	}
	for _, anno := range response.Annotations {
		trackMissingResponses(anno)
	}
	encodedResult, err := json.Marshal(response)

	if checkError(err, w, request.RequestInfo, len(request.IPs), "v2", tStart) {
		return
	}
	fmt.Fprint(w, string(encodedResult))
	if geoloader.IsLegacy(request.Date) {
		// Label this v2 (api) and legacy (dataset)
		latencyStats(request.RequestInfo, "v2-legacy", len(request.IPs), tStart)
	} else {
		// Label this v2 (api) and geolite2 (dataset)
		latencyStats(request.RequestInfo, "v2-geolite2", len(request.IPs), tStart)
	}
}

func handleNewOrOld(w http.ResponseWriter, tStart time.Time, jsonBuffer []byte) {
	// Check API version of the request
	wrapper := api.RequestWrapper{}
	err := json.Unmarshal(jsonBuffer, &wrapper)
	if err != nil {
		handleOld(w, tStart, jsonBuffer)
	} else {
		switch wrapper.RequestType {
		case v2.RequestTag:
			handleV2(w, tStart, jsonBuffer)
		default:
			if checkError(errors.New("Unknown Request Type"), w, "newOrOld", 0, "", tStart) {
				return
			}
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
func GetMetadataForSingleIP(request *api.RequestData) (result api.GeoData, err error) {
	metrics.TotalLookups.Inc()
	ann, err := manager.GetAnnotator(request.Timestamp)
	if err != nil {
		return
	}
	requestIP := request.IP
	if strings.HasPrefix(request.IP, "2002:") {
		requestIP = Ip6to4(request.IP)
	}
	err = ann.Annotate(requestIP, &result)
	return
}
