package legacy

// GeoIPDBTypes enum in GeoIP.h
const (
	GEOIP_COUNTRY_EDITION            = 1
	GEOIP_REGION_EDITION_REV0        = 7
	GEOIP_CITY_EDITION_REV0          = 6
	GEOIP_ORG_EDITION                = 5
	GEOIP_ISP_EDITION                = 4
	GEOIP_CITY_EDITION_REV1          = 2
	GEOIP_REGION_EDITION_REV1        = 3
	GEOIP_PROXY_EDITION              = 8
	GEOIP_ASNUM_EDITION              = 9
	GEOIP_NETSPEED_EDITION           = 10
	GEOIP_DOMAIN_EDITION             = 11
	GEOIP_COUNTRY_EDITION_V6         = 12
	GEOIP_LOCATIONA_EDITION          = 13
	GEOIP_ACCURACYRADIUS_EDITION     = 14
	GEOIP_CITYCONFIDENCE_EDITION     = 15
	GEOIP_CITYCONFIDENCEDIST_EDITION = 16
	GEOIP_LARGE_COUNTRY_EDITION      = 17
	GEOIP_LARGE_COUNTRY_EDITION_V6   = 18
	GEOIP_ASNUM_EDITION_V6           = 21
	GEOIP_ISP_EDITION_V6             = 22
	GEOIP_ORG_EDITION_V6             = 23
	GEOIP_DOMAIN_EDITION_V6          = 24
	GEOIP_LOCATIONA_EDITION_V6       = 25
	GEOIP_REGISTRAR_EDITION          = 26
	GEOIP_REGISTRAR_EDITION_V6       = 27
	GEOIP_USERTYPE_EDITION           = 28
	GEOIP_USERTYPE_EDITION_V6        = 29
	GEOIP_CITY_EDITION_REV1_V6       = 30
	GEOIP_CITY_EDITION_REV0_V6       = 31
	GEOIP_NETSPEED_EDITION_REV1      = 32
	GEOIP_NETSPEED_EDITION_REV1_V6   = 33
)

// GeoIPOptions enum in GeoIP.h
const (
	GEOIP_STANDARD     = 0
	GEOIP_MEMORY_CACHE = 1
	GEOIP_CHECK_CACHE  = 2
	GEOIP_INDEX_CACHE  = 4
	GEOIP_MMAP_CACHE   = 8
)