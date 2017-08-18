// Only files including IPv4, IPv6, and Location (in english)
// will be read and parsed into lists.
package parser

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"strconv"
	"regexp"
)

const mapMax = 200000
const LocationNumColumns = 13

// LocationNode defines Location databases
type LocationNode struct {
	GeonameID       int
	ContinentCode string
	CountryName   string
	MetroCode     int64
	CityName      string
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
		return nil, nil, errors.New("Corrupted Data")
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if len(record) != LocationNumColumns {
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
		match,_ := regexp.MatchString("^[A-Z]*$",record[2])
		if match == true{
			newNode.ContinentCode = record[2]
		}else{
			log.Println("Continent code should be all capitals and no numbers") 
			return nil, nil, errors.New("Corrupted Data: continent code should be all caps")
		}
		match,_ = regexp.MatchString("^[a-zA-Z]*$",record[5])
		if match == true{
			newNode.CountryName = record[5]
		}else{
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
