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
	"regexp"
	"strconv"
	"strings"
)

const ipNumColumnsGlite2 = 10
const locationNumColumnsGlite2 = 13
const ipNumColumnsGliteLatest = 3
const mapMax = 200000
const gLiteLatestPrefix = "GeoLiteCity-Blocks"
const gLite2CityPrefix = "GeoLite2-City-Blocks"

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
	list := []IPNode{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	// Skip first line
	_, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return list, errors.New("Empty input data")
	}

	if file == "GeoLiteCity-Blocks.csv" {
		_, err := r.Read()
		if err == io.EOF {
			log.Println("Empty input data")
			return list, errors.New("Empty input data")
		}
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		var newNode IPNode
		switch {
		case strings.HasPrefix(file, gLiteLatestPrefix):
			err = checkColumnLength(record, ipNumColumnsGliteLatest)
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
			index, err := validateGeoId(record[2], idMap)
			if err != nil {
				return nil, err
			}
			newNode.LocationIndex = index
			list = append(list, newNode)
		case strings.HasPrefix(file, gLite2CityPrefix):
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
			index, err := validateGeoId(record[1], idMap)
			if err != nil {
				if backupIndex, err := validateGeoId(record[2], idMap); err == nil {
					index = backupIndex
				} else {
					log.Println("Couldn't get a valid Geoname id!", record)
					//TODO: Add a prometheus metric here
				}
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
		default:
			log.Println("Unaccepted csv file provided: ", file)
			return list, errors.New("Unaccepted csv file provided")
		}
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

// Returns the index of geonameId within idMap
func validateGeoId(gnid string, idMap map[int]int) (int, error) {
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
				log.Println("GeonameID should be a number")
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
		match, _ := regexp.MatchString(`^[^\d]*$`, record[5])
		if match {
			newNode.CountryName = record[5]
		} else {
			log.Println("Country name should be letters only: ", record[5])
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
