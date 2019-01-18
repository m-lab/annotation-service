package handler_test

// TODO 201901 - TestStatusServiceUnavailable()
// TODO 201901 - TestStatusInternalError()

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/handler"
	"github.com/m-lab/annotation-service/manager"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestAnnotate(t *testing.T) {
	tests := []struct {
		ip   string
		time string
		res  string
	}{
		{"1.4.128.0", "625600", `{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583","latitude":42.1,"longitude":-73.1},"ASN":null}`},
		{"This will be an error.", "1000", "invalid IP address"},
	}
	// TODO - make and use an annotator generator
	manager.CurrentAnnotator = &geolite2.GeoDataset{
		IP4Nodes: []geolite2.IPNode{
			{
				IPAddressLow:  net.IPv4(0, 0, 0, 0),
				IPAddressHigh: net.IPv4(255, 255, 255, 255),
				LocationIndex: 0,
				PostalCode:    "10583",
				Latitude:      42.1,
				Longitude:     -73.1,
			},
		},
		IP6Nodes: []geolite2.IPNode{
			{
				IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				LocationIndex: 0,
				PostalCode:    "10583",
				Latitude:      42.1,
				Longitude:     -73.1,
			},
		},
		LocationNodes: []geolite2.LocationNode{
			{
				CityName: "Not A Real City", RegionCode: "ME",
			},
		},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))
		handler.Annotate(w, r)
		body := w.Body.String()
		if string(body) != test.res {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}

func TestValidateAndParse(t *testing.T) {
	tests := []struct {
		req *http.Request
		res *api.RequestData
		err error
	}{
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=127.0.0.1&since_epoch=fail", nil),
			res: nil,
			err: errors.New("invalid time"),
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=fail&since_epoch=10", nil),
			res: nil,
			err: errors.New("invalid IP address"),
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=127.0.0.1&since_epoch=10", nil),
			res: &api.RequestData{IP: "127.0.0.1", IPFormat: 4, Timestamp: time.Unix(10, 0)},
			err: nil,
		},
		{
			req: httptest.NewRequest("GET",
				"http://example.com/annotate?ip_addr=2620:0:1003:1008:5179:57e3:3c75:1886&since_epoch=10", nil),
			res: &api.RequestData{IP: "2620:0:1003:1008:5179:57e3:3c75:1886", IPFormat: 6, Timestamp: time.Unix(10, 0)},
			err: nil,
		},
	}
	for _, test := range tests {
		res, err := handler.ValidateAndParse(test.req)
		if diff := deep.Equal(res, test.res); diff != nil {
			t.Error(diff)
		}
		if diff := deep.Equal(err, test.err); diff != nil {
			t.Error(diff)
		}
	}

}

type badReader int

func (badReader) Read(_ []byte) (n int, err error) {
	return 0, errors.New("Bad Reader")
}

func TestBatchValidateAndParse(t *testing.T) {
	timeCon, _ := time.Parse(time.RFC3339, "2002-10-02T15:00:00Z")
	tests := []struct {
		source io.Reader
		res    []api.RequestData
		err    error
	}{
		{
			source: badReader(0),
			res:    nil,
			err:    errors.New("Bad Reader"),
		},
		{
			source: bytes.NewBufferString(`{`),
			res:    nil,
			err:    errors.New("unexpected end of JSON input"),
		},
		{
			source: bytes.NewBufferString(`[]`),
			res:    []api.RequestData{},
			err:    nil,
		},
		{
			source: bytes.NewBufferString(`[{"ip": "Bad IP", "timestamp": "2002-10-02T15:00:00Z"}]`),
			res:    nil,
			err:    errors.New("invalid IP address"),
		},
		{
			source: bytes.NewBufferString(`[{"ip": "127.0.0.1", "timestamp": "2002-10-02T15:00:00Z"},` +
				`{"ip": "2620:0:1003:1008:5179:57e3:3c75:1886", "timestamp": "2002-10-02T15:00:00Z"}]`),
			res: []api.RequestData{
				{IP: "127.0.0.1", IPFormat: 4, Timestamp: timeCon},
				{IP: "2620:0:1003:1008:5179:57e3:3c75:1886", IPFormat: 6, Timestamp: timeCon},
			},
			err: nil,
		},
	}
	for i, test := range tests {
		jsonBuffer, err := ioutil.ReadAll(test.source)
		if err != nil {
			if err.Error() != test.err.Error() {
				log.Printf("Expected %T\n", test.err)
				t.Error(err)
			}
			continue
		}
		res, err := handler.BatchValidateAndParse(jsonBuffer)
		if diff := deep.Equal(res, test.res); diff != nil {
			t.Error(diff)
		}
		// TODO use deep.Equal for testing errors?
		if err != nil && test.err == nil || err == nil && test.err != nil {
			t.Errorf("Test %d: Expected %+v, got %+v.", i, test.err, err)
			continue
		}
		if err != nil && test.err != nil && err.Error() != test.err.Error() {
			t.Errorf("Test %d: Expected %+v, got %+v.", i, test.err, err)
			continue
		}
	}

}

func TestBatchAnnotate(t *testing.T) {
	tests := []struct {
		body string
		res  string
		alt  string // Alternate valid result
	}{
		{
			body: "{",
			res:  "unexpected end of JSON input",
			alt:  "",
		},
		{
			body: `[{"ip": "127.0.0.1", "timestamp": "2017-08-25T13:31:12.149678161-04:00"},
                               {"ip": "2620:0:1003:1008:5179:57e3:3c75:1886", "timestamp": "2017-08-25T14:32:13.149678161-04:00"}]`,
			res: `{"127.0.0.1ov94o0":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583"},"ASN":null},"2620:0:1003:1008:5179:57e3:3c75:1886ov97hp":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583"},"ASN":null}}`,
			// TODO - remove alt after updating json annotations to omitempty.
			alt: `{"127.0.0.1ov94o0":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"ASN":null},"2620:0:1003:1008:5179:57e3:3c75:1886ov97hp":{"Geo":{"region":"ME","city":"Not A Real City","postal_code":"10583","latitude":0,"longitude":0},"ASN":{}}}`,
		},
	}
	// TODO - make a test utility in geolite2 package.
	manager.CurrentAnnotator = &geolite2.GeoDataset{
		IP4Nodes: []geolite2.IPNode{
			{
				IPAddressLow:  net.IPv4(0, 0, 0, 0),
				IPAddressHigh: net.IPv4(255, 255, 255, 255),
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		IP6Nodes: []geolite2.IPNode{
			{
				IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		LocationNodes: []geolite2.LocationNode{
			{
				CityName: "Not A Real City", RegionCode: "ME",
			},
		},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/batch_annotate", strings.NewReader(test.body))
		handler.BatchAnnotate(w, r)
		body := w.Body.String()
		if string(body) != test.res && string(body) != test.alt {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}

// TODO(JM) Update the test code/data here once we are no longer
// returning a canned response
func TestGetMetadataForSingleIP(t *testing.T) {
	tests := []struct {
		req *api.RequestData
		res api.GeoData
	}{
		{
			req: &api.RequestData{IP: "127.0.0.1", IPFormat: 4, Timestamp: time.Unix(0, 0)},
			res: api.GeoData{
				Geo: &api.GeolocationIP{City: "Not A Real City", PostalCode: "10583"},
				ASN: nil},
		},
	}
	manager.CurrentAnnotator = &geolite2.GeoDataset{
		IP4Nodes: []geolite2.IPNode{
			{
				IPAddressLow:  net.IPv4(0, 0, 0, 0),
				IPAddressHigh: net.IPv4(255, 255, 255, 255),
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		IP6Nodes: []geolite2.IPNode{
			{
				IPAddressLow:  net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				IPAddressHigh: net.IP{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				LocationIndex: 0,
				PostalCode:    "10583",
			},
		},
		LocationNodes: []geolite2.LocationNode{
			{
				CityName: "Not A Real City",
			},
		},
	}
	for _, test := range tests {
		res, _ := handler.GetMetadataForSingleIP(test.req)
		if diff := deep.Equal(res, test.res); diff != nil {
			t.Error(diff)
		}
	}
}

func TestE2ELoadMultipleDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test that uses GCS")
	}
	manager.InitDataset()
	tests := []struct {
		ip   string
		time string
		res  string
	}{
		//		{"1.4.128.0", "1199145600", `{"Geo":{"continent_code":"AS","country_code":"TH","country_code3":"THA","country_name":"Thailand","region":"40","city":"Bangkok","latitude":13.754,"longitude":100.501},"ASN":{}}`},
		//		{"1.5.190.1", "1420070400", `{"Geo":{"continent_code":"AS","country_code":"JP","country_code3":"JPN","country_name":"Japan","region":"40","city":"Tokyo","latitude":35.685,"longitude":139.751},"ASN":{}}`},
		{"1.9.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"MY","country_name":"Malaysia","region":"14","city":"Kuala Lumpur","postal_code":"50400","latitude":3.149,"longitude":101.697},"ASN":{}}`},
		{"1.22.128.0", "1512086400", `{"Geo":{"continent_code":"AS","country_code":"IN","country_name":"India","region":"DL","city":"Delhi","postal_code":"110062","latitude":28.6667,"longitude":77.2167},"ASN":{}}`},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		r := &http.Request{}
		r.URL, _ = url.Parse("/annotate?ip_addr=" + url.QueryEscape(test.ip) + "&since_epoch=" + url.QueryEscape(test.time))
		log.Println("Calling handler")
		handler.Annotate(w, r)
		i := 20
		for w.Result().StatusCode != http.StatusOK {
			log.Println("Try again", w.Result().Status, w.Body.String())
			i--
			if i == 0 {
				break
			}
			time.Sleep(1 * time.Second)
			w = httptest.NewRecorder()
			handler.Annotate(w, r)
		}

		body := w.Body.String()
		log.Println(body)

		if string(body) != test.res {
			t.Errorf("\nGot\n__%s__\nexpected\n__%s__\n", body, test.res)
		}
	}
}
