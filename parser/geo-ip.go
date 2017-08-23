// Only files including IPv4, IPv6, and Location (in english)
// will be read and parsed into lists.
package parser

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
)

const ipNumColumnsGlite2 = 10
const locationNumColumnsGlite2 = 13
const ipNumColumnsGliteLatest = 9
const mapMax = 200000

// IPNode IPv4 and Block IPv6 databases
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

// Creates a List of nodes for either IPv4 or IPv6 databases.
func CreateIPList(reader io.Reader, idMap map[int]int, glite string) ([]IPNode, error) {
	list := []IPNode{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	// Skip first line
	_, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return list, errors.New("Empty input data")
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		var newNode IPNode
		if glite == "geolatest" {
			if len(record) != ipNumColumnsGliteLatest {
				log.Println("Incorrect number of columns in IP list", ipNumColumnsGliteLatest, " got: ", len(record), record)
				return nil, errors.New("Corrupted Data: wrong number of columns")

			}
			var newNode IPNode
			_, err := strconv.Atoi(record[0])
			if err != nil {
				log.Println("startIpNum should be a number")
				return nil, errors.New("Corrupted Data: startIpNum should be a number")
			}
			newNode.IPAddressLow = net.ParseIP(record[0])
			_, err = strconv.Atoi(record[1])
			if err != nil {
				log.Println("endIpNum should be a number")
				return nil, errors.New("Corrupted Data: endIpNum should be a number")
			}
			newNode.IPAddressHigh = net.ParseIP(record[1])
			index, err := validateGeoId(record[2], idMap)
			if err != nil {
				return nil, err
			}
			newNode.LocationIndex = index
		} else if glite == "geolite2" {
			if len(record) != ipNumColumnsGlite2 {
				log.Println("Incorrect number of columns in IP list", ipNumColumnsGlite2, " got: ", len(record), record)
				return nil, errors.New("Corrupted Data: wrong number of columns")

			}
			_, _, err := net.ParseCIDR(record[0])
			if err != nil {
				log.Println("Incorrect CIDR form: ", record[0])
				return nil, errors.New("Corrupted Data: invalid CIDR IP range")
			}
			newNode.IPAddressLow = RangeCIDR(record[0], "low")
			newNode.IPAddressHigh = RangeCIDR(record[0], "high")
			index, err := validateGeoId(record[1], idMap)
			if err != nil {
				return nil, err
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
		}
		list = append(list, newNode)
	}
	return list, nil
}

func RangeCIDR(cidr, bound string) net.IP {
	ip,ipnet,_ := net.ParseCIDR(cidr)
	if bound == "low"{
		return ip
	}
	mask := ipnet.Mask 
	for x,_ := range ip{
		if len(mask) == 4 {
			if x < 12 {
				ip[x] |= 0
			}else{
				ip[x] |= ^mask[x-12]
			}
		}else{
			ip[x] |= ^mask[x]
		}
	}
	return ip
}
func validateGeoId(field string, idMap map[int]int) (int, error) {
	geonameId, err := strconv.Atoi(field)
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
		match, _ := regexp.MatchString("^[a-zA-Z]*$", record[5])
		if match {
			newNode.CountryName = record[5]
		} else {
			log.Println("Country name should be letters only")
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
