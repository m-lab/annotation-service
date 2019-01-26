# annotation-service

[![GoDoc](https://godoc.org/github.com/m-lab/annotation-service?status.svg)](https://godoc.org/github.com/m-lab/annotation-service) [![Build Status](https://travis-ci.org/m-lab/annotation-service.svg?branch=master)](https://travis-ci.org/m-lab/annotation-service) [![Coverage Status](https://coveralls.io/repos/github/m-lab/annotation-service/badge.svg?branch=master)](https://coveralls.io/github/m-lab/annotation-service?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/m-lab/annotation-service)](https://goreportcard.com/report/github.com/m-lab/annotation-service)

Annotation integration service provides geolocation for IPv4 and IPv6 MaxMind databases from Google Cloud Storage.

## API
### v1
This is the original, deprecated API, which includes an unwrapped array of
RequestData objects. It's use is discouraged.  Please use the v2 API, which
has better support for batch requests, including returning the date of the
 Annotator used to provide the annotations.

### v2
The v2 api introduces a standard wrapper struct, beginning with a string 
specifying the version identifier, and an Info string that may include 
arbitrary request info, e.g. for use in tracing or debugging.
It is described in the api/v2 package in the api/v2 directory.  The recommended
 GetAnnotations function is only available in the v2 package.

### Response contents
Annotatation service will respond with the following data:
- IP Address range
- Postal Code
- Latitude
- Longitude
- Continent Code
- Country Code 
- Country Name
- Metro Code
- City Name

---

# Code structure
The code is divided into the following packages:

* api - handles external API, including GetAnnotations() call which handles composing and sending requests, with retries.
* handler - receives incoming requests, handles marshalling, unmarshalling, interpretation of requests.
* loader - handles files downloads and decompression
* geoloader - maintains directory of available MaxMind files, and selects which file(s) to use for a given request.  (Needs a lot of renaming)
* geolite2v2 and legacy - handle details of interpreting MaxMind files and creating annotators.
Currently this is divided into two packages, but should be merged.
* manager - handles caching of Annotators

---

## Maxmind Dataset details

If an annotation request is dated prior to August 2017, location data will be
derived from MaxMind GeoLiteLatest databases. Otherwise, data will be provided
by MaxMind GeoLite2 databases. The discrepencies between provided databases are
provided below.

MaxMind GeoLiteLatest databases include:

  1. GeoLiteCity-Blocks.csv
    - StartIPNum IPv4
    - EndIPNum  IPv4
    - GeonameID
  1. GeoLiteCity-Location.csv
    - GeonameID
    - Country Code
    - City
    - Postal Code
    - Logitude
    - Latitude
    - MetroCode

MaxMind GeoLite2 databases include:

  1. GeoLite2-City-Blocks-IPv4.csv & GeoLite2-City-Blocks-IPv6.csv
    - IP network (CIDR Format)
    - GeonameID (identifies end user location)
    - Registered Country Geoname ID (identifies country where IP address is
      registered to an ISP)
    - Latitude
    - Longitude
  1. GeoLite2-City-Locations-en.csv
    - GeonameID
    - Continent Code
    - Country ISO
    - Country Name
    - City Name
    - Metro Code

GeonameID is the same with Registered Country Geoname ID most of time, but with some exceptions.
Either GeonameID or Registered Country Geoname ID could be not available for some IP addresses.

Important discrepencies to note include:

1. GeoLite2 databases provides a network IP range in CIDR format while
   GeoLiteLatest databases provide an IP range in decimal format.
1. GeoLite2 provides both end user location as well as country registration
   information while GeoLiteLatest includes only end user location.
   www.maxmind.com/en/geoip2-precision-city-service

## The directory package

The directory package provides tools to create a list of CompositeAnnotator (CA) wrappers.
directory.GetAnnotator(date time.Time) returns an appropriate CA for the given date.

### CompositeAnnotator

CompositeAnnotator encapsulates multiple component api.Annotator objects, and satisfies the
api.Annotator interface.

MergeAnnotators takes two or more []api.Annotator, and merges them, creating CompositeAnnotators for
each distinct date, using the most recent Annotator from each list prior to that date.

### Directory

Directory wraps a []api.Annotator, and provides the GetAnnotator(date time.Time) function.

### CachingLoader

CachingLoader specifies the interface provided by loaders that load and cache a list of annotators.

1. Maintains list of loaded Annotator objects.
1. Refreshes the list of loaded objects on demand.

```
type CachingLoader interface {
  UpdateCache() error
  Fetch() []Annotator
}
```

