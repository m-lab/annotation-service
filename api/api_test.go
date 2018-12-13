package api_test

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestRequestWrapper(t *testing.T) {
	req := v2.Request{RequestType: "foobar"}

	bytes, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	wrapper := api.RequestWrapper{}
	err = json.Unmarshal(bytes, &wrapper)
	if err != nil {
		t.Fatal(err)
	}
	switch wrapper.RequestType {
	case req.RequestType:
		err = json.Unmarshal(bytes, &req)
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("wrong request type:", wrapper.RequestType)
	}

	oldReq := []api.RequestData{{"IP", 4, time.Time{}}}
	bytes, err = json.Marshal(oldReq)
	if err != nil {
		t.Fatal(err)
	}
	err = json.Unmarshal(bytes, &wrapper)
	if err == nil {
		t.Fatal("Should have produced json unmarshal error")
	}
}
