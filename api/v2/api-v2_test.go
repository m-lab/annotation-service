package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
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
	expectedJson := `{"AnnotatorDate":"2018-12-05T00:00:00Z","Annotations":{"147.1.2.3":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"Network":{}},"8.8.8.8":{"Geo":{"continent_code":"NA","country_code":"US","country_name":"United States","latitude":37.751,"longitude":-97.822},"Network":{}}}}`
	callCount := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			fmt.Fprint(w, expectedJson)
		}
		callCount++
	}))
	url := ts.URL

	//url = "https://annotator-dot-mlab-sandbox.appspot.com/batch_annotate"
	ips := []string{"8.8.8.8", "147.1.2.3"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := api.GetAnnotations(ctx, url, time.Now(), ips, "reqInfo")
	if err == nil {
		t.Fatal("Should have timed out")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err = api.GetAnnotations(ctx, url, time.Now(), ips, "reqInfo")
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

func TestSomeErrors(t *testing.T) {
	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "body message")
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		callCount++
	}))
	url := ts.URL

	ips := []string{"8.8.8.8", "147.1.2.3"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := api.GetAnnotations(ctx, url, time.Now(), ips, "reqInfo")
	if callCount != 1 {
		t.Errorf("Should have been %d calls to server: %d", 1, callCount)
	}
	if err == nil {
		t.Fatal("Should have produced an error")
	}
	if !strings.Contains(err.Error(), "body message") {
		t.Error("Expected err containing body message", err)
	}
	if !strings.Contains(err.Error(), "Internal Server Error") {
		t.Error("Expected err containing Internal Server Error", err)
	}
}
