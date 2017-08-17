// Only files including IPv4 IPv6 and Location (in english)
// will be read and parsed into lists.
package parser

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"io"
	"strconv"
)

const IPColumnNum = 10
const LocationColumnNum = 13

// BlockNode defintes Block IPv4 and Block IPv6 databases
type BlockNode struct {
	IPAddress  string
	Geoname    int
	PostalCode string
	Latitude   float64
	Longitude  float64
}

// LocationNode defines Location databases
type LocationNode struct {
	Geoname       int
	ContinentCode string
	CountryName   string
	MetroCode     int64
	CityName      string
}

func NewBlockNode(ipa string, gn int, pc string, lat, long float64) BlockNode {
	return BlockNode{ipa, gn, pc, lat, long}
}

func NewLocNode(gn int, cc, cn string, mc int64, ctn string) LocationNode {
	return LocationNode{gn, cc, cn, mc, ctn}
}

// Unzips file and calls functions to create IPv4 IPv6 and LocLists
func Unzip(r *zip.Reader) ([]BlockNode, []BlockNode, []LocationNode, error) {
	var IPv4List []BlockNode
	var IPv6List []BlockNode
	var LocationList []LocationNode
	// Add metricsIPv4List, IPv6List, LocationList
	for _, f := range r.File {
		if len(f.Name) >= len("GeoLite2-City-Blocks-IPv4.csv") && f.Name[len(f.Name)-len("GeoLite2-City-Blocks-IPv4.csv"):] == "GeoLite2-City-Blocks-IPv4.csv" {
			rc, err := f.Open()
			if err != nil {
				return IPv4List, IPv6List, LocationList, err
			}
			defer rc.Close()
			IPv4List, err = CreateIPList(rc)
			if err != nil {
				return IPv4List, IPv6List, LocationList, err
			}
		}
		if len(f.Name) >= len("GeoLite2-City-Blocks-IPv6.csv") && f.Name[len(f.Name)-len("GeoLite2-City-Blocks-IPv6.csv"):] == "GeoLite2-City-Blocks-IPv6.csv" {
			rc, err := f.Open()
			if err != nil {
				return IPv4List, IPv6List, LocationList, err
			}
			defer rc.Close()
			IPv6List, err = CreateIPList(rc)
			if err != nil {
				return IPv4List, IPv6List, LocationList, err
			}
		}
		if len(f.Name) >= len("GeoLite2-City-Locations-en.csv") && f.Name[len(f.Name)-len("GeoLite2-City-Locations-en.csv"):] == "GeoLite2-City-Locations-en.csv" {
			rc, err := f.Open()
			if err != nil {
				return IPv4List, IPv6List, LocationList, err
			}
			defer rc.Close()
			LocationList, err = CreateLocationList(rc)
			if err != nil {
				return IPv4List, IPv6List, LocationList, err
			}
		}
	}
	// TODO: Add metrics for error cases
	if IPv4List == nil || IPv6List == nil || LocationList == nil {
		return IPv4List, IPv6List, LocationList, errors.New("Corrupted Data")
	}
	return IPv4List, IPv6List, LocationList, nil
}

// Creates a List of nodes for either IPv4 or IPv6 databases.
func CreateIPList(reader io.Reader) ([]BlockNode, error) {
	list := []BlockNode{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	// Skip first line
	_, err := r.Read()
	if err == io.EOF {
		return list, errors.New("Corrupted Data")
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if len(record) != IPColumnNum {
			return list, errors.New("Corrupted Data")
		}
		var newNode BlockNode
		newNode.IPAddress = record[0]
		newNode.Geoname, err = strconv.Atoi(record[1])
		if err != nil {
			if len(record[0]) > 0 {
				return list, errors.New("Corrupted Data")
			}
		}
		newNode.PostalCode = record[6]
		newNode.Latitude, err = strconv.ParseFloat(record[7], 64)
		if err != nil {
			if len(record[7]) > 0 {
				return list, errors.New("Corrupted Data")
			}
		}
		newNode.Longitude, err = strconv.ParseFloat(record[8], 64)
		if err != nil {
			if len(record[8]) > 0 {
				return list, errors.New("Corrupted Data")
			}
		}
		list = append(list, newNode)
	}
	return list, nil
}

// Creates list for location databases
func CreateLocationList(reader io.Reader) ([]LocationNode, error) {
	list := []LocationNode{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	// Skip the first line
	_, err := r.Read()
	if err == io.EOF {
		return list, errors.New("Corrupted Data")
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if len(record) != LocationColumnNum {
			return list, errors.New("Corrupted Data")
		}
		var newNode LocationNode
		newNode.Geoname, err = strconv.Atoi(record[0])
		if err != nil {
			if len(record[0]) > 0 {
				return list, errors.New("Corrupted Data")
			}
		}
		newNode.ContinentCode = record[2]
		newNode.CountryName = record[5]
		newNode.MetroCode, err = strconv.ParseInt(record[11], 10, 64)
		if err != nil {
			if len(record[11]) > 0 {
				return list, errors.New("Corrupted Data")
			}
		}
		newNode.CityName = record[10]
		list = append(list, newNode)
	}
	return list, nil
}
