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
func Unzip(r *zip.Reader) ([]BlockNode, []BlockNode, []LocNode, error) {
	var listIPv4 []BlockNode
	var listIPv6 []BlockNode
	var listLoc []LocNode

	for _, f := range r.File {
		if len(f.Name) >= len("GeoLite2-City-Blocks-IPv4.csv") && f.Name[len(f.Name)-len("GeoLite2-City-Blocks-IPv4.csv"):] == "GeoLite2-City-Blocks-IPv4.csv" {
			rc, err := f.Open()
			if err != nil {
				fmt.Println("error opening GeoLite2-Country-Blocks-IPv4.csv")
				return listIPv4, listIPv6, listLoc, err
			}
			defer rc.Close()
			listIPv4, err = CreateIPList(rc)
			if err != nil {
				fmt.Println(err)
				fmt.Println("error creating IPv4")
				return listIPv4, listIPv6, listLoc, err
			}
			if listIPv4 == nil {
				fmt.Println("BAAAAD")
			}
		}
		if len(f.Name) >= len("GeoLite2-City-Blocks-IPv6.csv") && f.Name[len(f.Name)-len("GeoLite2-City-Blocks-IPv6.csv"):] == "GeoLite2-City-Blocks-IPv6.csv" {
			rc, err := f.Open()
			if err != nil {
				fmt.Println("error opening GeoLite2-Country-Blocks-IPv6.csv")
				return listIPv4, listIPv6, listLoc, err
			}
			defer rc.Close()
			listIPv6, err = CreateIPList(rc)
			if err != nil {
				fmt.Println("error creating IPv6")
				return listIPv4, listIPv6, listLoc, err
			}
		}
		if len(f.Name) >= len("GeoLite2-City-Locations-en.csv") && f.Name[len(f.Name)-len("GeoLite2-City-Locations-en.csv"):] == "GeoLite2-City-Locations-en.csv" {
			rc, err := f.Open()
			if err != nil {
				fmt.Println("error opening GeoLite2-Country-Locations-en.csv")
				return listIPv4, listIPv6, listLoc, err
			}
			defer rc.Close()
			listLoc, err = CreateLocList(rc)
			if err != nil {
				fmt.Println("error creating location list")
				return listIPv4, listIPv6, listLoc, err
			}
		}
	}
	if listIPv4 == nil {
		fmt.Println("IPV4 is null")
	}
	if listIPv6 == nil {
		fmt.Println("IPV6 is null")
	}
	if listLoc == nil {
		fmt.Println("listLOC is null")
	}

	if listIPv4 == nil || listIPv6 == nil || listLoc == nil {
		return listIPv4, listIPv6, listLoc, errors.New("Incomplete Data")
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
		fmt.Println("File is empty")
		log.Fatal(err)
	}
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if len(record) != 10 {
			fmt.Println(record)
		}
		var newNode BlockNode
		newNode.IPAddress = record[0]
		newNode.Geoname, err = strconv.Atoi(record[1])
		if err != nil {
			newNode.Geoname = 0
		}
		newNode.PostalCode = record[6]
		newNode.Latitude, err = strconv.ParseFloat(record[7], 64)
		if err != nil {
			fmt.Println("no latitude")
			fmt.Println("--------------------")
			fmt.Println(record)
			fmt.Println("--------------------")
			fmt.Println(len(record))
			fmt.Println("--------------------")
			return list, err
		}
		newNode.Longitude, err = strconv.ParseFloat(record[8], 64)
		if err != nil {
			fmt.Println("no longitude")
			fmt.Println("--------------------")
			fmt.Println(record)
			fmt.Println("--------------------")
			fmt.Println(len(record))
			fmt.Println("--------------------")
			return list, err

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
