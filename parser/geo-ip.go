// Only files including IPv4, IPv6, and Location (in english)
// will be read and parsed into lists.
package parser

import (
	"bytes"
	"errors"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
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

// locationNode defines Location databases
type LocationNode struct {
	GeonameID     int
	ContinentCode string
	CountryCode   string
	CountryName   string
	MetroCode     int64
	CityName      string
}

// The GeoDataset struct bundles all the data needed to search and
// find data into one common structure
type GeoDataset struct {
	IP4Nodes      []IPNode       // The IPNode list containing IP4Nodes
	IP6Nodes      []IPNode       // The IPNode list containing IP6Nodes
	LocationNodes []LocationNode // The location nodes corresponding to the IPNodes
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
// lookupGeoId("17",locationIdMap) would return (2,nil).
// TODO: Add error metrics
func lookupGeoId(gnid string, idMap map[int]int) (int, error) {
	geonameId, err := strconv.Atoi(gnid)
	if err != nil {
		return 0, errors.New("Corrupted Data: geonameID should be a number")
	}
	loadIndex, ok := idMap[geonameId]
	if !ok {
		log.Println("geonameID not found ", geonameId)
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

func checkCaps(str, field string) (string, error) {
	match, _ := regexp.MatchString("^[0-9A-Z]*$", str)
	if match {
		return str, nil
	} else {
		log.Println(field, "should be all capitals and no punctuation: ", str)
		output := strings.Join([]string{"Corrupted Data: ", field, " should be all caps and no punctuation"}, "")
		return "", errors.New(output)

	}
}

// Returns nil if two nodes are equal
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
					// if theres a gap inbetween imediately nested IP's
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
