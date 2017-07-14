package annotator

import (
	"testing"
	"net/http/httptest"
)

func Test_lookupAndRespond(t *testing.T) {
	tests:= [] struct{
		ip string 
		time int64
		res string
	}{
		{"1.0.0.0",625600, "time: 625600 \n[\n  {\"ip\": \"1.0.0.0\", \"type\": \"STRING\"},\n  {\"country\": \"Australia\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"AU\", \"type\": \"STRING\"},\n]"},
		{"0.0.0.0",625600,"ERROR, IP ADDRESS NOT FOUND\n"},
	}
	for _, test := range tests {
		w := httptest.NewRecorder()
		lookupAndRespond(w,test.ip,test.time)
		body := w.Body.String() 
		if string(body)!= test.res{
			t.Errorf("Got \"%s\", wanted \"%s\"!",body,test.res) 
		}
	}
}
