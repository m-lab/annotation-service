// Only files including IPv4, IPv6, and Location (in english)
// will be read and parsed into lists.
package parser

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"strconv"
)

const LocationNumColumns = 13

// LocationNode defines Location databases
type LocationNode struct {
	Geoname       int
	ContinentCode string
	CountryName   string
	MetroCode     int64
	CityName      string
}

// Creates list for location databases
func CreateLocationList(reader io.Reader) ([]LocationNode, map[int]int, error) {
	idMap := make(map[int]int, 200000)
	list := []LocationNode{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	// Skip the first line
	_, err := r.Read()
	if err == io.EOF {
		log.Println("Empty file") 
		return nil, nil, errors.New("Corrupted Data")
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if len(record) != LocationNumColumns {
			log.Println("Incorrect number of columns in Location list") 
			return nil, nil, errors.New("Corrupted Data")
		}
		var newNode LocationNode
		newNode.Geoname, err = strconv.Atoi(record[0])
		if err != nil {
			if len(record[0]) > 0 {
				log.Println("Geoname was a number") 
				return nil, nil, errors.New("Corrupted Data")
			}
		}
		newNode.ContinentCode = record[2]
		newNode.CountryName = record[5]
		newNode.MetroCode, err = strconv.ParseInt(record[11], 10, 64)
		if err != nil {
			if len(record[11]) > 0 {
				log.Println("MetroCode is not a number")
				return nil, nil, errors.New("Corrupted Data")
			}
		}
		newNode.CityName = record[10]
		list = append(list, newNode)
		idMap[newNode.Geoname] = len(list) -1
	}
	return list, idMap, nil
}
