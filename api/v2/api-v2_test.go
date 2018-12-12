package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-test/deep"
	v2 "github.com/m-lab/annotation-service/api/v2"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestDoRequest(t *testing.T) {
	expectedJson := `{"AnnotatorDate":"2018-12-05T00:00:00Z","Annotations":{"147.1.2.3":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"ASN":{}},"8.8.8.8":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"ASN":{}}}}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, expectedJson)
	}))
	url := ts.URL

	//url = "https://annotator-dot-mlab-sandbox.appspot.com/batch_annotate"
	ips := []string{"8.8.8.8", "147.1.2.3"}
	resp, err := v2.DoRequest(context.Background(), url, time.Now(), ips)
	if err != nil {
		t.Fatal(err)
	}

	expectedResponse := v2.Response{}
	err = json.Unmarshal([]byte(expectedJson), &expectedResponse)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(expectedResponse, *resp); diff != nil {
		t.Error(diff)
	}
}
