package handler_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/handler"
)

func TestFindGeofileForTime(t *testing.T) {
	tests := []struct {
		timestamp time.Time
		resStr    string
		resInt    int
		err       error
	}{
		{
			timestamp: time.Date(2016, 4, 7, 11, 46, 12, 67, time.UTC),
			resStr:    "Maxmind/2016/03/08/20160308T080000Z-GeoLiteCity-latest.zip",
			resInt:    1,
			err:       nil,
		},
		{
			timestamp: time.Date(2016, 4, 8, 11, 46, 12, 67, time.UTC),
			resStr:    "Maxmind/2016/03/08/20160308T080000Z-GeoLiteCity-latest.zip",
			resInt:    1,
			err:       nil,
		},
		{
			timestamp: time.Date(2016, 4, 9, 11, 46, 12, 67, time.UTC),
			resStr:    "Maxmind/2016/04/08/20160408T080000Z-GeoLiteCity-latest.zip",
			resInt:    1,
			err:       nil,
		},
	}
	for _, test := range tests {
		name, ver, err := handler.FindGeofileForTime(test.timestamp)
		if !reflect.DeepEqual(name, test.resStr) || !reflect.DeepEqual(ver, test.resInt) || !reflect.DeepEqual(err, test.err) {
			t.Errorf("Expected %s, %d, %s, got %s, %d, %s", test.resStr, test.resInt, test.err, name, ver, err)
		}
	}
}
