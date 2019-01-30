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

func TestExtractDateFromFilename(t *testing.T) {
	date, err := api.ExtractDateFromFilename("Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz")
	if date.Year() != 2017 || date.Month() != 5 || date.Day() != 8 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %v, %+v.", 20170508, date, err)
	}

	date2, err := api.ExtractDateFromFilename("Maxmind/2017/10/05/20171005T033334Z-GeoLite2-City-CSV.zip")
	if date2.Year() != 2017 || date2.Month() != 10 || date2.Day() != 5 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %v, %+v.", 20171005, date2, err)
	}
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

type fakeAnn struct {
	api.Annotator
	startDate time.Time
}

func (f *fakeAnn) AnnotatorDate() time.Time {
	return f.startDate
}

func newFake(date string) *fakeAnn {
	d, err := time.Parse("20060102", date)
	if err != nil {
		log.Println(err)
	}
	return &fakeAnn{startDate: d}
}

func TestCompositeAnnotator_String(t *testing.T) {
	tests := []struct {
		name       string
		annotators []api.Annotator
		want       string
	}{
		{"simple", []api.Annotator{newFake("20100203"), newFake("20110304")}, "[20100203][20110304]"},
		// TODO: Add test cases.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ca := api.NewCompositeAnnotator(tt.annotators)

			if got := ca.(api.CompositeAnnotator).String(); got != tt.want {
				t.Errorf("CompositeAnnotator.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
