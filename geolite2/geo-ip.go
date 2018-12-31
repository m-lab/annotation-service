// Package geolite2 contains code for loading and parsing GeoLite2 datasets.
// Only files including IPv4, IPv6, and Location (in english)
// will be read and parsed into lists.
package geolite2

import (
	"bytes"
	"errors"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/metrics"
)

const mapMax = 200000

// IPNode defines IPv4 and IPv6 databases
type IPNode struct {
	IPAddressLow  net.IP
	IPAddressHigh net.IP
	LocationIndex int // Index to slice of locations
	PostalCode    string
	Latitude      float64
	Longitude     float64
}

// LocationNode defines Location databases
type LocationNode struct {
	GeonameID     int
	ContinentCode string
	CountryCode   string
	CountryName   string
	RegionCode    string
	RegionName    string
	MetroCode     int64
	CityName      string
}

// The GeoDataset struct bundles all the data needed to search and
// find data into one common structure
type GeoDataset struct {
	start         time.Time      // Date from which to start using this dataset
	IP4Nodes      []IPNode       // The IPNode list containing IP4Nodes
	IP6Nodes      []IPNode       // The IPNode list containing IP6Nodes
	LocationNodes []LocationNode // The location nodes corresponding to the IPNodes
}

// ErrNodeNotFound is returned when we can't find data in our system.
// TODO SearchBinary and associated code should go in legacy, NOT here.
// Need to clean up handler code first, though.
var ErrNodeNotFound = errors.New("node not found")

// SearchBinary does a binary search for a list element.
func (ds *GeoDataset) SearchBinary(ipLookUp string, IsIP4 bool) (p IPNode, e error) {
	list := ds.IP6Nodes
	if IsIP4 {
		list = ds.IP4Nodes
	}
	start := 0
	end := len(list) - 1

	userIP := net.ParseIP(ipLookUp)
	for start <= end {
		median := (start + end) / 2
		if bytes.Compare(userIP, list[median].IPAddressLow) >= 0 && bytes.Compare(userIP, list[median].IPAddressHigh) <= 0 {
			return list[median], nil
		}
		if bytes.Compare(userIP, list[median].IPAddressLow) > 0 {
			start = median + 1
		} else {
			end = median - 1
		}
	}
	return p, ErrNodeNotFound
}

// ConvertIPNodeToGeoData takes a parser.IPNode, plus a list of
// locationNodes. It will then use that data to fill in a GeoData
// struct and return its pointer.
// TODO make this unexported
func convertIPNodeToGeoData(ipNode IPNode, locationNodes []LocationNode) api.GeoData {
	locNode := LocationNode{}
	if ipNode.LocationIndex >= 0 {
		locNode = locationNodes[ipNode.LocationIndex]
	}
	return api.GeoData{
		Geo: &api.GeolocationIP{
			ContinentCode: locNode.ContinentCode,
			CountryCode:   locNode.CountryCode,
			CountryCode3:  "", // missing from geoLite2 ?
			CountryName:   locNode.CountryName,
			Region:        locNode.RegionCode,
			MetroCode:     locNode.MetroCode,
			City:          locNode.CityName,
			AreaCode:      0, // new geoLite2 does not have area code.
			PostalCode:    ipNode.PostalCode,
			Latitude:      ipNode.Latitude,
			Longitude:     ipNode.Longitude,
		},
		ASN: &api.IPASNData{},
	}

}

func (ds *GeoDataset) GetAnnotationOld(request *api.RequestData) (api.GeoData, error) {
	return api.GeoData{}, errors.New("not implemented")
}

// GetAnnotation looks up the IP address and returns the corresponding GeoData
// TODO - improve the format handling.  Perhaps pass in a net.IP ?
func (ds *GeoDataset) GetAnnotation(ips string) (api.GeoData, error) {
	// TODO - this block of code repeated in legacy.
	ip := net.ParseIP(ips)
	if ip == nil {
		metrics.BadIPTotal.Inc()
		return api.GeoData{}, errors.New("cannot parse ip")
	}
	format := 4
	if ip.To4() == nil {
		format = 6
	}

	var node IPNode
	err := errors.New("unknown IP format")
	node, err = ds.SearchBinary(ips, format == 4)

	if err != nil {
		// ErrNodeNotFound is super spammy - 10% of requests, so suppress those.
		if err != ErrNodeNotFound {
			log.Println(err, ips)
		}
		//TODO metric here
		return api.GeoData{}, err
	}

	return convertIPNodeToGeoData(node, ds.LocationNodes), nil
}

// AnnotatorDate returns the date that the dataset was published.
// TODO implement actual dataset time!!
func (ds *GeoDataset) AnnotatorDate() time.Time {
	return ds.start
}

// Verify column length
func checkNumColumns(record []string, size int) error {
	if len(record) != size {
		log.Println("Incorrect number of columns in IP list", size, " got: ", len(record), record)
		return errors.New("Corrupted Data: wrong number of columns")
	}
	return nil
}

// Finds provided geonameID within idMap and returns the index in idMap
// locationIdMap := map[int]int{
//	609013: 0,
//	104084: 4,
//	17:     4,
// }
// lookupGeoID("17",locationIdMap) would return (2,nil).
// TODO: Add error metrics
func lookupGeoID(gnid string, idMap map[int]int) (int, error) {
	geonameID, err := strconv.Atoi(gnid)
	if err != nil {
		return 0, errors.New("Corrupted Data: geonameID should be a number")
	}
	loadIndex, ok := idMap[geonameID]
	if !ok {
		log.Println("geonameID not found ", geonameID)
		return 0, errors.New("Corrupted Data: geonameId not found")
	}
	return loadIndex, nil
}

func stringToFloat(str, field string) (float64, error) {
	flt, err := strconv.ParseFloat(str, 64)
	if err != nil {
		if len(str) > 0 {
			log.Println(field, " was not a number")
			output := strings.Join([]string{"Corrupted Data: ", field, " should be an int"}, "")
			return 0, errors.New(output)
		}
	}
	return flt, nil
}

var capsRE = regexp.MustCompile("^[0-9A-Z]*$")

// checkCaps ensures that field name contains only upper case A-Z and digits 0-9.
func checkCaps(str, field string) (string, error) {
	if capsRE.MatchString(str) {
		return str, nil
	}
	log.Println(field, "should be all capitals and no punctuation: ", str)
	output := strings.Join([]string{"Corrupted Data: ", field, " should be all caps and no punctuation"}, "")
	return "", errors.New(output)
}

// IsEqualIPNodes returns nil if two nodes are equal
// Used by the search package
func IsEqualIPNodes(expected, node IPNode) error {
	if !((node.IPAddressLow).Equal(expected.IPAddressLow)) {
		output := strings.Join([]string{"IPAddress Low inconsistent\ngot:", node.IPAddressLow.String(), " \nwanted:", expected.IPAddressLow.String()}, "")
		log.Println(output)
		return errors.New(output)
	}
	if !((node.IPAddressHigh).Equal(expected.IPAddressHigh)) {
		output := strings.Join([]string{"IPAddressHigh inconsistent\ngot:", node.IPAddressHigh.String(), " \nwanted:", expected.IPAddressHigh.String()}, "")
		log.Println(output)
		return errors.New(output)
	}
	if node.LocationIndex != expected.LocationIndex {
		output := strings.Join([]string{"LocationIndex inconsistent\ngot:", strconv.Itoa(node.LocationIndex), " \nwanted:", strconv.Itoa(expected.LocationIndex)}, "")
		log.Println(output)
		return errors.New(output)
	}
	if node.PostalCode != expected.PostalCode {
		output := strings.Join([]string{"PostalCode inconsistent\ngot:", node.PostalCode, " \nwanted:", expected.PostalCode}, "")
		log.Println(output)
		return errors.New(output)
	}
	if node.Latitude != expected.Latitude {
		output := strings.Join([]string{"Latitude inconsistent\ngot:", floatToString(node.Latitude), " \nwanted:", floatToString(expected.Latitude)}, "")
		log.Println(output)
		return errors.New(output)
	}
	if node.Longitude != expected.Longitude {
		output := strings.Join([]string{"Longitude inconsistent\ngot:", floatToString(node.Longitude), " \nwanted:", floatToString(expected.Longitude)}, "")
		log.Println(output)
		return errors.New(output)
	}
	return nil
}

func floatToString(num float64) string {
	return strconv.FormatFloat(num, 'f', 6, 64)
}

// TODO(gfr) What are list and stack?
// handleStack finds the proper place in the stack for the new node.
// `stack` holds a stack of nested IP ranges not yet resolved.
// `list` is the complete list of flattened IPNodes.
func handleStack(stack, list []IPNode, newNode IPNode) ([]IPNode, []IPNode) {
	// Stack is not empty aka we're in a nested IP
	if len(stack) != 0 {
		// newNode is no longer inside stack's nested IP's
		if lessThan(stack[len(stack)-1].IPAddressHigh, newNode.IPAddressLow) {
			// while closing nested IP's
			var pop IPNode
			pop, stack = stack[len(stack)-1], stack[:len(stack)-1]
			for ; len(stack) > 0; pop, stack = stack[len(stack)-1], stack[:len(stack)-1] {
				peek := stack[len(stack)-1]
				if lessThan(newNode.IPAddressLow, peek.IPAddressHigh) {
					// if there's a gap in between adjacent nested IP's,
					// complete the gap
					peek.IPAddressLow = PlusOne(pop.IPAddressHigh)
					peek.IPAddressHigh = minusOne(newNode.IPAddressLow)
					list = append(list, peek)
					break
				}
				peek.IPAddressLow = PlusOne(pop.IPAddressHigh)
				list = append(list, peek)
			}
		} else {
			// if we're nesting IP's
			// create begnning bounds
			lastListNode := &list[len(list)-1]
			lastListNode.IPAddressHigh = minusOne(newNode.IPAddressLow)

		}
	}
	stack = append(stack, newNode)
	list = append(list, newNode)
	return stack, list
}

func moreThan(a, b net.IP) bool {
	return bytes.Compare(a, b) > 0
}

func lessThan(a, b net.IP) bool {
	return bytes.Compare(a, b) < 0
}

// PlusOne adds one to a net.IP.
func PlusOne(a net.IP) net.IP {
	a = append([]byte(nil), a...)
	var i int
	for i = 15; a[i] == 255; i-- {
		a[i] = 0
	}
	a[i]++
	return a
}
func minusOne(a net.IP) net.IP {
	a = append([]byte(nil), a...)
	var i int
	for i = 15; a[i] == 0; i-- {
		a[i] = 255
	}
	a[i]--
	return a
}
