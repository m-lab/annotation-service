package parser

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"reflect"
	"strconv"
)

const (
	ipNumColumnsGlite1       = 3
	locationNumColumnsGlite1 = 9
	gLite1Prefix             = "GeoLiteCity"
)

// Glite1HelpNode defines IPNode data defined inside
// GeoLite1 Location files
type gLite1HelpNode struct {
	Latitude   float64
	Longitude  float64
	PostalCode string
}

// Create Location list, map, and Glite1HelpNode for GLite1 databases
// GLiteHelpNode contains information that populate fields in IPNode
func LoadLocListGLite1(reader io.Reader) ([]LocationNode, []gLite1HelpNode, map[int]int, error) {
	r := csv.NewReader(reader)
	idMap := make(map[int]int, mapMax)
	list := []LocationNode{}
	glite := []gLite1HelpNode{}
	// Skip the first 2 lines
	_, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, nil, nil, errors.New("Empty input data")
	}
	_, err = r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, nil, nil, errors.New("Empty input data")
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if len(record) != locationNumColumnsGlite1 {
			log.Println("Incorrect number of columns in Location list\n\twanted: ", locationNumColumnsGlite1, " got: ", len(record), record)
			return nil, nil, nil, errors.New("Corrupted Data: wrong number of columns")
		}
		var lNode LocationNode
		lNode.GeonameID, err = strconv.Atoi(record[0])
		if err != nil {
			return nil, nil, nil, errors.New("Corrupted Data: GeonameID should be a number")
		}
		lNode.CountryCode, err = checkAllCaps(record[1], "Country code")
		if err != nil {
			return nil, nil, nil, err
		}
		lNode.CityName = record[3]
		var gNode gLite1HelpNode
		gNode.PostalCode = record[4]
		gNode.Latitude, err = stringToFloat(record[5], "Latitude")
		if err != nil {
			return nil, nil, nil, err
		}
		gNode.Longitude, err = stringToFloat(record[6], "Longitude")
		if err != nil {
			return nil, nil, nil, err
		}

		list = append(list, lNode)
		glite = append(glite, gNode)
		idMap[lNode.GeonameID] = len(list) - 1
	}
	return list, glite, idMap, nil
}

// Creates a List of IPNodes
func LoadIPListGLite1(reader io.Reader, idMap map[int]int, glite1 []gLite1HelpNode) ([]IPNode, error) {
	g1IP := []string{"startIpNum", "endIpNum", "locId"}
	list := []IPNode{}
	r := csv.NewReader(reader)
	// Skip first line
	title, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, errors.New("Empty input data")
	}
		// Skip 2nd line, which contains column labels
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
			newNode.IPAddressLow, err = int2ip(record[0])
			if err != nil {
				return nil, err
			}
			newNode.IPAddressHigh, err = int2ip(record[1])
			if err != nil {
				return nil, err
			}
			// Look for GeoId within idMap and return index
			index, err := lookupGeoId(record[2], idMap)
			if err != nil {
				return nil, err
			}
			newNode.LocationIndex = index
			if glite1[index].Latitude != 0 {
				newNode.Latitude = glite1[index].Latitude
			}
			if glite1[index].Longitude != 0 {
				newNode.Longitude = glite1[index].Longitude
			}
			newNode.PostalCode = glite1[index].PostalCode
			list = append(list, newNode)
		}
	return list, nil
}


