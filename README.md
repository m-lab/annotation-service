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
  2. GeoLiteCity-Location.csv
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
  2. GeoLite2-City-Locations-en.csv
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
2. GeoLite2 provides both end user location as well as country registration
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

## TTL-cache
The TTL-cache provides a cache of objects managed by TTL.
When an object's TTL expires, the object's Free() function is called, and it is removed from the cache.
Each object's TTL is reset whenever an object is Loaded or returned by Get().

Each object provides
  ```
  Load()
  Free()
  ```

The cache provides:
  ```
  Add(Key string, loader func() (interface, error))
  Get(Key string) (interface, error)
  ```
The cache periodically enumerates all objects, and Frees those that have exceeded TTL.
The interval is set to 1/5 of the cache's TTL.

### Errors:

`ErrObjectLoading` is returned by Get when an annotator is loading but not yet available.

`ErrObjectUnloaded` is stored in entry.err when the object is nil, and no-one is loading it.

`ErrObjectLoadFailed` is returned when a requested object load failed.

`ErrCacheFull` is returned when there is a request to load an object, but the cache is full.

`ErrTooManyLoading` is returned when there is a request to load an object, but too many other objects are currently being loaded.

### Use
The annototor-service uses the TTL cache to manage low level Annotator objects, including GeoLite2 annotators, Geolite legacy v4 and v6 annotators, and ASN annotators.

These annotators are generally aggregated into CompositeAnnotators that maintain the keys to the individual annotators, and call all of the annotators sequentially to annotate a single object.

