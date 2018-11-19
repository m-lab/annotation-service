package handler

import (
	"os"
	"regexp"
	"time"
)

// This is the bucket containing maxmind files.
var BucketName = "downloader-" + os.Getenv("GCLOUD_PROJECT")

// This is the regex used to filter for which files we want to consider acceptable for using with Geolite2
var GeoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

var LatestDatasetDate time.Time

const (
	MaxmindPrefix = "Maxmind/" // Folder containing the maxmind files
)
