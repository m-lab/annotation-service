package handler

import (
	"os"
	"regexp"
)

// This is the bucket containing maxmind files.
var BucketName = "downloader-" + os.Getenv("GCLOUD_PROJECT")

// This is the regex used to filter for which files we want to consider acceptable for using with Geolite2
var GeoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)

// This is the regex used to filter for which files we want to consider acceptable for using with legacy dataset
var GeoLegacyRegex = regexp.MustCompile(`.*-GeoLiteCity.dat.*`)
var GeoLegacyv6Regex = regexp.MustCompile(`.*-GeoLiteCityv6.dat.*`)

// DatasetNames are list of datasets sorted in lexographical order in downloader bucket.
var DatasetNames []string

const (
	MaxmindPrefix = "Maxmind/" // Folder containing the maxmind files

	// This is the date we have the first GeoLite2 dataset.
	// Any request earlier than this date using legacy binary datasets
	// later than this date using GeoLite2 datasets
	GeoLite2CutOffDate = "August 15, 2017"

	// This is the base in which we should encode the timestamp when we
	// are creating the keys for the mapt to return for batch requests
	encodingBase = 36
)
