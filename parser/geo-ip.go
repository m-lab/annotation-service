// Only files including IPv4, IPv6, and Location (in english)
// will be read and parsed into lists.
package parser

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
)

const ipNumColumns = 10
const locationNumColumns = 13
const mapMax = 200000

// IPNode IPv4 and Block IPv6 databases
type IPNode struct {
	IPAddress     string
	LocationIndex int // Index to slice of locations
	PostalCode    string
	Latitude      float64
	Longitude     float64
}

// LocationNode defines Location databases
type LocationNode struct {
	GeonameID     int
	ContinentCode string
	CountryName   string
	MetroCode     int64
	CityName      string
}

// Creates a List of nodes for either IPv4 or IPv6 databases.
func CreateIPList(reader io.Reader, idMap map[int]int) ([]IPNode, error) {
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
		if len(record) != ipNumColumns {
			log.Println("Incorrect number of columns in IP list")
			return nil, errors.New("Corrupted Data: wrong number of columns")
		}
		var newNode IPNode
		newNode.IPAddress = record[0]
		geonameId, err := strconv.Atoi(record[1])
		if err != nil {
			log.Println("geonameID should be a number")
			return nil, errors.New("Corrupted Data: geonameID should be a number")
		}
		loadIndex, ok := idMap[geonameId]
		if !ok {
			log.Println("geonameID not found ", geonameId)
			return nil, errors.New("Corrupted Data: geonameId not found")
		}
		newNode.LocationIndex = loadIndex
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
	return list, nil
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
		if len(record) != locationNumColumns {
			log.Println("Incorrect number of columns in Location list")
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
		match, _ := regexp.MatchString("^[A-Z]*$", record[2])
		if match {
			newNode.ContinentCode = record[2]
		} else {
			log.Println("Continent code should be all capitals and no numbers")
			return nil, nil, errors.New("Corrupted Data: continent code should be all caps")
		}
		match, _ = regexp.MatchString("^[a-zA-Z]*$", record[5])
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
