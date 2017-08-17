# annotation-service
Annotation integration service for M-Lab data.

Downloader package reads in a GeoLite2-City database from GCS. 

Parser package creates a searchable list for geoLocation for IPv4 and IPv6
databases. 

File: GeoLite2-City-Blocks-IPv4.csv
Columns: network  geoname_id  registered_country_geoname_id
represented_country_geoname_id  is_anonymous_proxy  is_satellite_provider
postal_code latitude  longitude accuracy_radius

File: GeoLite2-City-Blocks-IPv6.csv
Columns: network  geoname_id  registered_country_geoname_id
represented_country_geoname_id  is_anonymous_proxy  is_satellite_provider
postal_code latitude  longitude accuracy_radius

File: GeoLite2-City-Locations-en.csv
Columns: network geoname_id  registered_country_geoname_id represented_country_geoname_id
is_anonymous_proxy  is_satellite_provider postal_code latitude  longitude
accuracy_radius

