// Only files including IPv4, IPv6, and Location (in english)
// will be read and parsed into lists.
package parser

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"math"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

const (
	ipNumColumnsGlite2       = 10
	locationNumColumnsGlite2 = 13
	gLite2Prefix             = "GeoLite2-City"

	ipNumColumnsGlite1       = 3
	locationNumColumnsGlite1 = 9
	gLite1Prefix             = "GeoLiteCity"

	mapMax = 200000
)

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
	MetroCode     int64
	CityName      string
}

// Creates a List of IPNodes
func CreateIPList(reader io.Reader, idMap map[int]int, file string) ([]IPNode, error) {
	g1IP := []string{"startIpNum", "endIpNum", "locId"}
	list := []IPNode{}
	r := csv.NewReader(reader)
	// Skip first line
	title, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, errors.New("Empty input data")
	}
	switch {
	case strings.HasPrefix(file, gLite1Prefix):
		// Skip 2nd line
		title, err = r.Read()
		if err == io.EOF {
			log.Println("Empty input data")
			return nil, errors.New("Empty input data")
		}
		if !reflect.DeepEqual(g1IP, title) {
			log.Println("Improper data format got: ", title, " wanted: ", g1IP)
			return nil, errors.New("Improper data format")
		}
		for {
			// Example:
			// GLite1 : record = [16777216,16777471,17]
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			err = checkColumnLength(record, ipNumColumnsGlite1)
			if err != nil {
				return nil, err
			}
			var newNode IPNode
			newNode.IPAddressLow, err = Int2ip(record[0])
			if err != nil {
				return nil, err
			}
			newNode.IPAddressHigh, err = Int2ip(record[1])
			if err != nil {
				return nil, err
			}
			// Look for GeoId within idMap and return index
			index, err := lookupGeoId(record[2], idMap)
			if err != nil {
				return nil, err
			}
			newNode.LocationIndex = index
			list = append(list, newNode)
		}
	case strings.HasPrefix(file, gLite2Prefix):
		for {
			// Example:
			// GLite2 : record = [2a04:97c0::/29,2658434,2658434,0,0,47,8,100]
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			var newNode IPNode
			err = checkColumnLength(record, ipNumColumnsGlite2)
			if err != nil {
				return nil, err
			}
			lowIp, highIp, err := RangeCIDR(record[0])
			if err != nil {
				return nil, err
			}
			newNode.IPAddressLow = lowIp
			newNode.IPAddressHigh = highIp
			// Look for GeoId within idMap and return index
			index, err := lookupGeoId(record[1], idMap)
			if err != nil {
				index, err = lookupGeoId(record[2], idMap)
			}
			newNode.LocationIndex = index
			newNode.PostalCode = record[6]
			newNode.Latitude, err = stringToFloat(record[7], "Latitude")
			if err != nil {
				return nil, err
			}
			newNode.Longitude, err = stringToFloat(record[8], "Longitude")
			if err != nil {
				return nil, err
			}
			list = append(list, newNode)
		}
	default:
		log.Println("Unaccepted csv file provided: ", file)
		return list, errors.New("Unaccepted csv file provided")
	}
	return list, nil
}

// Verify column length
func checkColumnLength(record []string, size int) error {
	if len(record) != size {
		log.Println("Incorrect number of columns in IP list", size, " got: ", len(record), record)
		return errors.New("Corrupted Data: wrong number of columns")
	}
	return nil
}

// Converts integer to net.IPv4
func Int2ip(str string) (net.IP, error) {
	num, err := strconv.Atoi(str)
	if err != nil {
		log.Println("Provided IP should be a number")
		return nil, errors.New("Inputed string cannot be converted to a number")
	}
	ft := float64(num)
	if ft > math.Pow(2, 32) || num < 1 {
		log.Println("Provided IP should be in the range of 0.0.0.1 and 255.255.255.255 ", str)
	}
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, uint32(num))
	ip = net.IPv4(ip[0], ip[1], ip[2], ip[3])
	return ip, nil
}

// Finds the smallest and largest net.IP from a CIDR range
// Example: "1.0.0.0/24" -> 1.0.0.0 , 1.0.0.255
func RangeCIDR(cidr string) (net.IP, net.IP, error) {
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, nil, errors.New("Invalid CIDR IP range")
	}
	lowIp := make(net.IP, len(ip))
	copy(lowIp, ip)
	mask := ipnet.Mask
	for x, _ := range ip {
		if len(mask) == 4 {
			if x < 12 {
				ip[x] |= 0
			} else {
				ip[x] |= ^mask[x-12]
			}
		} else {
			ip[x] |= ^mask[x]
		}
	}
	return lowIp, ip, nil
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
		log.Println("geonameID should be a number")
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

func checkAllCaps(str, field string) (string, error) {
	match, _ := regexp.MatchString("^[A-Z]*$", str)
	if match {
		return str, nil
	} else {
		log.Println(field, "should be all capitals and no numbers")
		output := strings.Join([]string{"Corrupted Data: ", field, " should be all caps"}, "")
		return "", errors.New(output)

	}
}

// Creates list for location databases
// returns list with location data and a hashmap with index to geonameId
func CreateLocationList(reader io.Reader) ([]LocationNode, map[int]int, error) {
	idMap := make(map[int]int, mapMax)
	list := []LocationNode{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	// Skip the first line
	_, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, nil, errors.New("Empty input data")
	}
	if err != nil {
		log.Println("Error reading file")
		return nil, nil, errors.New("Error reading file")
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if len(record) != locationNumColumnsGlite2 {
			log.Println("Incorrect number of columns in Location list\n\twanted: ", locationNumColumnsGlite2, " got: ", len(record), record)
			return nil, nil, errors.New("Corrupted Data: wrong number of columns")
		}
		var newNode LocationNode
		newNode.GeonameID, err = strconv.Atoi(record[0])
		if err != nil {
			if len(record[0]) > 0 {
				log.Println("GeonameID should be a number ", record[0])
				return nil, nil, errors.New("Corrupted Data: GeonameID should be a number")
			}
		}
		newNode.ContinentCode, err = checkAllCaps(record[2], "Continent code")
		if err != nil {
			return nil, nil, err
		}
		newNode.CountryCode, err = checkAllCaps(record[4], "Country code")
		if err != nil {
			return nil, nil, err
		}
		match, _ := regexp.MatchString(`^[^0-9]*$`, record[5])
		if match {
			newNode.CountryName = record[5]
		} else {
			log.Println("Country name should be letters only : ", record[5])
			return nil, nil, errors.New("Corrupted Data: country name should be letters")
		}
		newNode.MetroCode, err = strconv.ParseInt(record[11], 10, 64)
		if err != nil {
			if len(record[11]) > 0 {
				log.Println("MetroCode should be a number")
				return nil, nil, errors.New("Corrupted Data: metrocode should be a number")
			}
		}
		newNode.CityName = record[10]
		list = append(list, newNode)
		idMap[newNode.GeonameID] = len(list) - 1
	}
	return list, idMap, nil
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
