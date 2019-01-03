package geoloader_test

import (
	"testing"
	"time"

	"github.com/m-lab/annotation-service/geoloader"
)

func date(date string, t *testing.T) time.Time {
	d, err := time.Parse("20060102", date)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDir(t *testing.T) {
	dir := geoloader.NewDirectory(10)
	dir.Insert(date("20170101", t), "file1")
	dir.Insert(date("20170202", t), "file2")
	dir.Insert(date("20170101", t), "file1a")
	dir.Insert(date("20170404", t), "file4")
	dir.Insert(date("20170303", t), "file3")

	if dir.LastFilenameEarlierThan(date("20170102", t)) != "file1" {
		t.Error("wrong date", dir.LastFilenameEarlierThan(time.Now()))
	}
	if dir.LastFilenameEarlierThan(date("20170305", t)) != "file3" {
		t.Error("wrong date", dir.LastFilenameEarlierThan(date("20170305", t)))
	}
	// Should always choose date prior to, not equal to, provided date.
	if dir.LastFilenameEarlierThan(date("20170303", t)) != "file2" {
		t.Error("wrong date", dir.LastFilenameEarlierThan(date("20170303", t)))
	}
	// For very early dates, should get the first available.
	if dir.LastFilenameEarlierThan(date("20100101", t)) != "file1" {
		t.Error("wrong date", dir.LastFilenameEarlierThan(date("20100101", t)))
	}
}

func TestBestAnnotatorName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	err := geoloader.UpdateArchivedFilenames()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		date string
		want string
	}{
		{"20170102", "Maxmind/2016/12/08/20161208T080000Z-GeoLiteCity.dat.gz"},
		{"20180809", "Maxmind/2018/08/08/20180808T050355Z-GeoLite2-City-CSV.zip"},
		{"20170814", "Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz"},
		{"20170902", "Maxmind/2017/09/01/20170901T004438Z-GeoLite2-City-CSV.zip"},
		{"20170906", "Maxmind/2017/09/01/20170901T004438Z-GeoLite2-City-CSV.zip"},
	}
	for _, tt := range tests {
		t.Run(tt.date, func(t *testing.T) {
			d := date(tt.date, t)
			if got := geoloader.BestAnnotatorName(d); got != tt.want {
				t.Errorf("%s -> %v, want %v", tt.date, got, tt.want)
			}
		})
	}
}
