//Only files including IPv4 IPv6 and Location (in english)
//will be read and parsed into lists.
package parser

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
)

// BlockNode defintes Block IPv4 and Block IPv6 databases
type BlockNode struct {
	IPAddress  string
	Geoname    int
	PostalCode string
	Latitude   float64
	Longitude  float64
}

// LocNode defines Location databases
type LocNode struct {
	Geoname       int
	ContinentCode string
	CountryName   string
	MetroCode     int64
	CityName      string
}

func NewBlockNode(ipa string, gn int, pc string, lat, long float64) BlockNode {
	return BlockNode{ipa, gn, pc, lat, long}
}
func NewLocNode(gn int, cc, cn string, mc int64, ctn string) LocNode {
	return LocNode{gn, cc, cn, mc, ctn}
}

//Unzips file and calls functions to create IPv4 IPv6 and LocLists
func Unzip(src string) ([]BlockNode, []BlockNode, []LocNode, error) {
	var listIPv4 []BlockNode
	var listIPv6 []BlockNode
	var listLoc []LocNode
	r, err := zip.OpenReader(src)
	if err != nil {
		return listIPv4, listIPv6, listLoc, err
	}
	defer r.Close()

	for _, f := range r.File {
		if f.Name == "GeoLite2-Country-Blocks-IPv4.csv" {
			rc, err := f.Open()
			if err != nil {
				return listIPv4, listIPv6, listLoc, err
			}
			defer rc.Close()
			listIPv4, err = CreateIPList(rc)
			if listIPv4 == nil {
				fmt.Println("IPv4 is nil")
			}
		}
		if f.Name == "GeoLite2-Country-Blocks-IPv6.csv" {
			rc, err := f.Open()
			if err != nil {
				return listIPv4, listIPv6, listLoc, err
			}
			defer rc.Close()
			listIPv6, err = CreateIPList(rc)
		}
		if f.Name == "GeoLite2-Country-Locations-en.csv" {
			rc, err := f.Open()
			if err != nil {
				return listIPv4, listIPv6, listLoc, err
			}
			defer rc.Close()
			listLoc, err = CreateLocList(rc)
		}
		if err != nil {
			return listIPv4, listIPv6, listLoc, err
		}
	}
	return listIPv4, listIPv6, listLoc, nil
}

//Creates a List of nodes for either IPv4 or IPv6 databases.
func CreateIPList(reader io.Reader) ([]BlockNode, error) {
	list := []BlockNode{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true

	//skip first line
	_, err := r.Read()
	if err == io.EOF {
		fmt.Println("beginning bad")
		log.Fatal(err)
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		var newNode BlockNode
		if len(record) != 10 {
			fmt.Println("rows are bad")
			return list, errors.New("Corrupted file")
		}
		newNode.IPAddress = record[0]
		newNode.Geoname, err = strconv.Atoi(record[1])
		if err != nil {
			newNode.Geoname = 0
		}
		newNode.PostalCode = record[6]
		newNode.Latitude, err = strconv.ParseFloat(record[7], 64)
		if err != nil {
			fmt.Println("bad lat")
			log.Fatal(err)
		}
		newNode.Longitude, err = strconv.ParseFloat(record[8], 64)
		if err != nil {
			fmt.Println("bad long")
			log.Fatal(err)
		}
		list = append(list, newNode)
	}
	return list, nil
}

//Creates list for location databases
func CreateLocList(reader io.Reader) ([]LocNode, error) {
	list := []LocNode{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true

	//skip the first line
	_, err := r.Read()
	if err == io.EOF {
		log.Fatal(err)
	}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		var newNode LocNode
		if len(record) != 13 {
			return list, errors.New("Corrupted file")
		}
		newNode.Geoname, err = strconv.Atoi(record[0])
		if err != nil {
			log.Fatal(err)
		}
		newNode.ContinentCode = record[2]
		newNode.CountryName = record[5]
		if len(record[11]) > 0 {
			newNode.MetroCode, err = strconv.ParseInt(record[11], 10, 64)
		} else {
			newNode.MetroCode = 0
		}
		if err != nil {
			log.Fatal(err)
		}
		newNode.CityName = record[10]
		list = append(list, newNode)
	}
	return list, nil

}
