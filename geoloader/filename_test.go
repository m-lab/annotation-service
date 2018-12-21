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
