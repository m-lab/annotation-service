# annotation-service
Annotation integration service provides geolocation for IPv4 and IPv6 MaxMind databases from Google Cloud Storage.

If an annotation request is dated prior to August 2017, location data will be derived from
MaxMind GeoLiteLatest databases. Otherwise, data will be provided by
MaxMind GeoLite2 databases. The discrepencies between provided databases are
provided below.

MaxMind GeoLiteLatest databases include:
  1. IP Block File
    - StartIPNum IPv4
    - EndIPNum  IPv4
    - GeonameID 
  2. Location File
    - GeonameID
    - Country Code
    - City
    - Postal Code
    - Logitude
    - Latitude
    - MetroCode

MaxMind GeoLite2 databases include:
  1. IP Block File
    - IP network (CIDR Format)
    - GeonameID (identifies end user location)
    - Registered Country Geoname ID (identifies country where IP address is
      registered to an ISP)
    - Latitude
    - Longitude
  2. Location File
    - GeonameID
    - Continent Code
    - Country ISO
    - Country Name
    - City Name
    - Metro Code

Important descrepencies to note include:
1. GeoLite2 databases provides a network IP range in CIDR format while
   GeoLiteLatest databases provide an IP range in decimal format.
2. GeoLite2 provides both end user location as well as country registration
   information while GeoLiteLatest includes only end user location. On average
   end user location and country registration are the same but there are
   exceptions of one or more missing. 
   www.maxmind.com/en/geoip2-precision-city-service

Annotatation service will respond with the following data:
- IP Address range
- Postal Code
- Latitude
- Longitude
- Continent Code
- Country Name
- Metro Code
- City Name
