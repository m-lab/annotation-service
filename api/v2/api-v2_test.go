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
	"github.com/m-lab/annotation-service/api/v2"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestDoRequest(t *testing.T) {
	expectedJson := `{"AnnotatorDate":"2018-12-05T00:00:00Z","Annotations":{"147.1.2.3":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"ASN":{}},"8.8.8.8":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"ASN":{}}}}`
	callCount := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case callCount < 3:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			fmt.Fprint(w, expectedJson)
		}
		callCount++
	}))
	url := ts.URL

	//url = "https://annotator-dot-mlab-sandbox.appspot.com/batch_annotate"
	ips := []string{"8.8.8.8", "147.1.2.3"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := api.GetAnnotations(ctx, url, time.Now(), ips)
	if err == nil {
		t.Fatal("Should have timed out")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err = api.GetAnnotations(ctx, url, time.Now(), ips)
	if err != nil {
		t.Fatal(err)
	}

	if callCount != 4 {
		t.Error("Should have been two calls to server.")
	}
	expectedResponse := api.Response{}
	err = json.Unmarshal([]byte(expectedJson), &expectedResponse)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(expectedResponse, *resp); diff != nil {
		t.Error(diff)
	}
}
