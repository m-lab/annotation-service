// Package legacy supports legacy MaxMind data lookups.
// TODO - should probably rename this legacy?
/* Go (cgo) interface to libgeoip
   originally forked from github.com/abh/geoip
*/
package legacy

/*
#cgo pkg-config: geoip
#include <stdio.h>
#include <errno.h>
#include <GeoIP.h>
#include <GeoIPCity.h>
//typedef GeoIP* GeoIP_pnt
*/
import "C"

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"
	"unsafe"

	"github.com/m-lab/go/rtx"
)

// This is the regex used to filter for which files we want to consider acceptable for using with legacy dataset
var geoLegacyv6Regex = regexp.MustCompile(`.*-GeoLiteCityv6.dat.*`)

// GeoIPOptions enum in GeoIP.h
const (
	Standard    = 0
	MemoryCache = 1
	CheckCache  = 2
	IndexCache  = 4
	MMapCache   = 8
)

var (
	// once is used to make sure loading the fips2iso map only happens once.
	once sync.Once
)

// GeoIP contains a single v4 or v6 dataset for a particular day.
type GeoIP struct {
	db *C.GeoIP

	isIPv4 bool
	name   string

	// We don't use GeoIP's thread-safe API calls, which means there is a
	// single global netmask variable that gets clobbered in the main
	// lookup routine.  Any calls which have _GeoIP_seek_record_gl need to
	// be wrapped in this mutex.
	mu sync.Mutex

	// Counter that how many times Free() was called.
	freeCalled uint32

	// fips2ISOMap is a local pointer to the package variable.
	fips2ISOMap map[string]subdivision
}

// Free the memory hold by GeoIP dataset. Mutex should be held for this operation.
func (gi *GeoIP) Free() {
	if gi == nil {
		log.Println("Attempt to free from nil GeoIP pointer")
		return
	}
	if gi.db == nil || gi.freeCalled >= 1 {
		log.Println("GeoIP db already nil")
		return
	}
	log.Println("free memory for legacy dataset: " + gi.name)
	C.GeoIP_delete(gi.db)
	gi.freeCalled++
	return
}

// GetFreeCached returns how many times Free() was called.
func (gi *GeoIP) GetFreeCalled() uint32 {
	return gi.freeCalled
}

// Open opens a DB. It is a default convenience wrapper around OpenDB.
func Open(filename string, datasetName string) (*GeoIP, error) {
	return OpenDB(filename, MemoryCache, datasetName)
}

// OpenDB opens a GeoIP database by filename with specified GeoIPOptions flag.
// All formats supported by libgeoip are supported though there are only
// functions to access some of the databases in this API.
func OpenDB(file string, flag int, datasetName string) (*GeoIP, error) {
	g := &GeoIP{}
	// Original code has "runtime.SetFinalizer(g, (*GeoIP).Free)" here
	// But it caused the loaded DB kicked out from memory immediately and
	// has to be loaded again. So we remove it for now.

	var err error

	if _, err := os.Stat(file); err != nil {
		return nil, fmt.Errorf("Error get Fileinfo of GeoIP database (%s): %s", file, err)
	}

	cbase := C.CString(file)
	defer C.free(unsafe.Pointer(cbase))

	g.db, err = C.GeoIP_open(cbase, C.int(flag))

	if err != nil {
		return nil, fmt.Errorf("Error opening GeoIP database (%s): %s", file, err)
	}

	if g.db == nil {
		return nil, fmt.Errorf("Didn't open GeoIP database (%s)", file)
	}

	C.GeoIP_set_charset(g.db, C.GEOIP_CHARSET_UTF8)
	g.name = datasetName
	g.freeCalled = 0
	g.isIPv4 = !geoLegacyv6Regex.MatchString(datasetName)

	once.Do(func() {
		// Load the fips-to-iso CSV mapping FIPS to ISO codes.
		var err error
		fips2ISOMap, err = parseFips2ISOMap(Fips2ISOMapFile)
		rtx.Must(err, "Could not parse fips-to-iso file")
	})
	g.fips2ISOMap = fips2ISOMap

	return g, nil
}

// SetCustomDirectory sets the default location for the GeoIP .dat files used when
// calling OpenType()
func SetCustomDirectory(dir string) {
	cdir := C.CString(dir)
	// GeoIP doesn't copy the string, so don't free it when we're done here.
	// defer C.free(unsafe.Pointer(cdir))
	C.GeoIP_setup_custom_directory(cdir)
}

// OpenTypeFlag opens a specified GeoIP database type in the default location with the
// specified GeoIPOptions flag. Constants are defined for each database type
// (for example GEOIP_COUNTRY_EDITION).
func OpenTypeFlag(dbType int, flag int) (*GeoIP, error) {
	g := &GeoIP{}
	// Original code has "runtime.SetFinalizer(g, (*GeoIP).Free)" here
	// removed for the same reason above.

	var err error

	g.db, err = C.GeoIP_open_type(C.int(dbType), C.int(flag))
	if err != nil {
		return nil, fmt.Errorf("Error opening GeoIP database (%d): %s", dbType, err)
	}

	if g.db == nil {
		return nil, fmt.Errorf("Didn't open GeoIP database (%d)", dbType)
	}

	C.GeoIP_set_charset(g.db, C.GEOIP_CHARSET_UTF8)
	g.freeCalled = 0
	return g, nil
}

// OpenType opens a specified GeoIP database type in the default location
// and the 'memory cache' flag. Use OpenTypeFlag() to specify flag.
func OpenType(dbType int) (*GeoIP, error) {
	return OpenTypeFlag(dbType, MemoryCache)
}

// GetOrg takes an IPv4 address string and returns the organization name for that IP.
// Requires the GeoIP organization database.
// TODO remove this code.
func (gi *GeoIP) GetOrg(ip string) string {
	name, _ := gi.GetName(ip)
	return name
}

// GetName works on the ASN, Netspeed, Organization and probably other
// databases, takes an IP string and returns a "name" and the netmask.
// TODO remove this code.
func (gi *GeoIP) GetName(ip string) (name string, netmask int) {
	if gi.db == nil {
		return
	}

	gi.mu.Lock()
	defer gi.mu.Unlock()

	cip := C.CString(ip)
	defer C.free(unsafe.Pointer(cip))
	cname := C.GeoIP_name_by_addr(gi.db, cip)

	if cname != nil {
		name = C.GoString(cname)
		defer C.free(unsafe.Pointer(cname))
		netmask = int(C.GeoIP_last_netmask(gi.db))
		return
	}
	return
}

// GeoIPRecord contains a single record for a particular IP block.
type GeoIPRecord struct {
	CountryCode   string
	CountryCode3  string
	CountryName   string
	Region        string
	City          string
	PostalCode    string
	Latitude      float32
	Longitude     float32
	MetroCode     int
	AreaCode      int
	CharSet       int
	ContinentCode string
}

// GetRecord returns the "City Record" for an IP address. Requires the GeoCity(Lite)
// database - http://www.maxmind.com/en/city
// Returns nil if IP is invalid, wrong type (v4/v6), or record is not found.
// TODO - consider returning different error codes.
func (gi *GeoIP) GetRecord(ip string, isIP4 bool) *GeoIPRecord {
	if gi.db == nil {
		return nil
	}

	if len(ip) == 0 || isIP4 != gi.isIPv4 {
		return nil
	}

	cip := C.CString(ip)
	defer C.free(unsafe.Pointer(cip))

	var record *C.GeoIPRecord
	// We are using a non-thread-safe API, so we grab the lock.
	// TODO - can we use the thread-safe API insteaad?
	gi.mu.Lock()
	if gi.isIPv4 {
		record = C.GeoIP_record_by_addr(gi.db, cip)
	} else {
		record = C.GeoIP_record_by_addr_v6(gi.db, cip)
	}
	gi.mu.Unlock()

	if record == nil {
		return nil
	}
	// defer C.free(unsafe.Pointer(record))
	defer C.GeoIPRecord_delete(record)
	rec := new(GeoIPRecord)
	rec.CountryCode = C.GoString(record.country_code)
	rec.CountryCode3 = C.GoString(record.country_code3)
	rec.CountryName = C.GoString(record.country_name)
	rec.Region = C.GoString(record.region)
	rec.City = C.GoString(record.city)
	rec.PostalCode = C.GoString(record.postal_code)
	rec.Latitude = float32(record.latitude)
	rec.Longitude = float32(record.longitude)
	rec.CharSet = int(record.charset)
	rec.ContinentCode = C.GoString(record.continent_code)

	if gi.db.databaseType != C.GEOIP_CITY_EDITION_REV0 {
		/* DIRTY HACK BELOW:
		   The GeoIPRecord struct in GeoIPCity.h contains an int32 union of metro_code and dma_code.
		   The union is unnamed, so cgo names it anon0 and assumes it's a 4-byte array.
		*/
		unionInt := (*int32)(unsafe.Pointer(&record.anon0))
		rec.MetroCode = int(*unionInt)
		rec.AreaCode = int(record.area_code)
	}

	return rec
}

// GetRegion returns the country code and region code for an IP address. Requires
// the GeoIP Region database.
func (gi *GeoIP) GetRegion(ip string) (string, string) {
	if gi.db == nil {
		return "", ""
	}

	cip := C.CString(ip)
	defer C.free(unsafe.Pointer(cip))

	gi.mu.Lock()
	region := C.GeoIP_region_by_addr(gi.db, cip)
	gi.mu.Unlock()

	if region == nil {
		return "", ""
	}

	countryCode := C.GoString(&region.country_code[0])
	regionCode := C.GoString(&region.region[0])
	defer C.free(unsafe.Pointer(region))

	return countryCode, regionCode
}

// GetRegionName returns the region name given a country code and region code
func GetRegionName(countryCode, regionCode string) string {

	cc := C.CString(countryCode)
	defer C.free(unsafe.Pointer(cc))

	rc := C.CString(regionCode)
	defer C.free(unsafe.Pointer(rc))

	region := C.GeoIP_region_name_by_code(cc, rc)
	if region == nil {
		return ""
	}

	// it's a static string constant, don't free this
	regionName := C.GoString(region)

	return regionName
}

// GetNameV6 is same as GetName() but for IPv6 addresses.
// TODO remove this code.
func (gi *GeoIP) GetNameV6(ip string) (name string, netmask int) {
	if gi.db == nil {
		return
	}

	gi.mu.Lock()
	defer gi.mu.Unlock()

	cip := C.CString(ip)
	defer C.free(unsafe.Pointer(cip))
	cname := C.GeoIP_name_by_addr_v6(gi.db, cip)

	if cname != nil {
		name = C.GoString(cname)
		defer C.free(unsafe.Pointer(cname))
		netmask = int(C.GeoIP_last_netmask(gi.db))
		return
	}
	return
}

// GetCountry takes an IPv4 address string and returns the country code for that IP
// and the netmask for that IP range.
func (gi *GeoIP) GetCountry(ip string) (cc string, netmask int) {
	if gi.db == nil {
		return
	}

	gi.mu.Lock()
	defer gi.mu.Unlock()

	cip := C.CString(ip)
	defer C.free(unsafe.Pointer(cip))
	ccountry := C.GeoIP_country_code_by_addr(gi.db, cip)

	if ccountry != nil {
		cc = C.GoString(ccountry)
		netmask = int(C.GeoIP_last_netmask(gi.db))
		return
	}
	return
}

// GetCountryV6 works the same as GetCountry except for IPv6 addresses, be sure to
// load a database with IPv6 data to get any results.
func (gi *GeoIP) GetCountryV6(ip string) (cc string, netmask int) {
	if gi.db == nil {
		return
	}

	gi.mu.Lock()
	defer gi.mu.Unlock()

	cip := C.CString(ip)
	defer C.free(unsafe.Pointer(cip))
	ccountry := C.GeoIP_country_code_by_addr_v6(gi.db, cip)
	if ccountry != nil {
		cc = C.GoString(ccountry)
		netmask = int(C.GeoIP_last_netmask(gi.db))
		return
	}
	return
}
