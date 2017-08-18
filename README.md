# annotation-service
Annotation integration service for M-Lab data.

Downloader package reads in a GeoLite2-City database from GCS. 

Parser package creates a searchable list for geoLocation for IPv4 and IPv6
databases. 

NOTE TO RUN TESTS LOCALLY: 
export
GOOGLE_APPLICATION_CREDENTIALS="/usr/local/google/home/shelbychen/go/src/github.com/m-lab/annotation-service/key.json"
&& export
APPENGINE_DEV_APPSERVER="/usr/local/google/home/shelbychen/google-cloud-sdk/bin/dev_appserver.py"
&& source "/usr/local/google/home/shelbychen/google-cloud-sdk/path.bash.inc" &&
go test -v ./...

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

