# annotation-service
Annotation integration service provides geolocation for IPv4 and IPv6 MaxMind databases from Google Cloud Storage.

Annotated geolocation data includes:
- IP Address range
- Postal Code
- Latitude
- Longitude
- Continent Code
- Country Name
- Metro Code
- City Name

GeoLite2City databases provides a network IP range in CIDR format.
GeoLiteLatest databases provide an IP range in decimal format.

GeoLite2City identifies end-user location (geoname_id) as well as the country
where the IP address is registered to the ISP (registered_country_geoname_id).
