# etl

| branch | travis-ci | coveralls |
|--------|-----------|-----------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/annotation-service.svg?branch=master)](https://travis-ci.org/m-lab/annotation-service) | [![Coverage Status](https://coveralls.io/repos/m-lab/annotation-service/badge.svg?branch=master)](https://coveralls.io/github/m-lab/annotation-service?branch=master) |

Annotation integration service provides geolocation for IPv4 and IPv6 MaxMind databases from Google Cloud Storage.

If an annotation request is dated prior to August 2017, location data will be derived from
MaxMind GeoLiteLatest databases. Otherwise, data will be provided by
MaxMind GeoLite2 databases. The discrepencies between provided databases are
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

Important descrepencies to note include:

1. GeoLite2 databases provides a network IP range in CIDR format while
   GeoLiteLatest databases provide an IP range in decimal format.
1. GeoLite2 provides both end user location as well as country registration
   information while GeoLiteLatest includes only end user location.
   www.maxmind.com/en/geoip2-precision-city-service

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

## Loading datasets

We will modify the LoadAll function to instead be a Loader class that:

1. Holds a source, filter, and loader
1. Maintains list of loaded Annotator objects.
1. Refreshes the list of loaded objects on demand.

The cache package will then contain a Generator class that maintain a list of these objects that in turn
maintain the complete collection of base Annotators, an Update method that will trigger updating all the
annotator lists, and a Generate method that creates a new AnnotatorCache

### Loader

Loader should be an interface, so that we can allow annotator providers to provide their own Loaders.  The geoloader package may also provide a generic NewLoader.

* func NewLoader(source, filter, loader) *Loader
* func (ld *Loader) Update()  // Updates the list of Annotators.

### Generator

* func (gen *Generator) AddLoader(loader *Loader)
* func (gen *Generator) Update()  // Reloads all lists
* func (gen *Generator) Generate() *AnnotatorCache // Creates a new cache.