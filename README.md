# annotation-service
Annotation integration service for M-Lab data.

Downloader package reads in a GeoLite2-City database from GCS. 

Parser package creates a searchable list for geoLocation for IPv4 and IPv6
databases. 

IPv4 and IPv6 Columns 
network  geoname_id  registered_country_geoname_id
represented_country_geoname_id  is_anonymous_proxy  is_satellite_provider
postal_code latitude  longitude accuracy_radius

GeoLite Block Columns 
network geoname_id  registered_country_geoname_id represented_country_geoname_id
is_anonymous_proxy  is_satellite_provider postal_code latitude  longitude
accuracy_radius

