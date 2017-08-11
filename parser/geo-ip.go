//Depending on whether user input was an IPv4 or IPv6 IPaddress,
//respective database file will be read in and a list of Nodes will be created
//Each node contains a geo-location and its range of IP addresses
package parser

import (
	"errors"
	"io"
	"net"
	"compress/gzip"
	"encoding/csv"
)

// Node defines the range of IP addresses per country
type BlockNode struct {
	IPAdress net.IPNet
	Geoname int
	PostalCode string
	Latitude string
	Longitude string 
}

func NewBlockNode(ipa net.IPNet,gn int, pc,lat,long string) BlockNode {
	return Node{ipa,gn,pc,lat,long}
}

func Unzip(reader io.Reader) *Reader{
	newReader, err := gzip.NewReader(reader) 
	if err != nil{
		log.Fatal(err) 
	}
	defer newReader.Close() 
	return newReader() 
}

//Creates a List of nodes for either IPv4 or IPv6 databases.
func CreateList(reader io.Reader, IPVersion int, zipFile bool) ([]Node, error) {
	list := []Node{}
	r.TrimLeadingSpace = true
	
	//if needs to be unzipped 
	if zipFile{
		r := csv.NewReader(reader)
	}else{
		r := csv.NewReader(reader)
	}
	
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		var newNode Node
		if IPVersion == 4 {
			if len(record) != 6 {
				return list, errors.New("Corrupted file")
			}
			newNode.LowRangeBin = net.ParseIP(record[0])
			newNode.HighRangeBin = net.ParseIP(record[1])
			newNode.CountryAbrv = record[4]
			newNode.CountryName = record[5]

			if newNode.LowRangeBin.To4() == nil {
				return list, errors.New("Low range IP invalid")
			}
			if newNode.HighRangeBin.To4() == nil {
				return list, errors.New("High range IP invalid")
			}
		}
		if IPVersion == 6 {
			if len(record) != 12 {
				return list, errors.New("Corrupted file")
			}
			newNode.LowRangeBin = net.ParseIP(record[0])
			newNode.HighRangeBin = net.ParseIP(record[1])
			newNode.CountryAbrv = record[4]
			newNode.CountryName = "N/A"

			if newNode.LowRangeBin.To16() == nil {
				return list, errors.New("Low range IP invalid")
			}
			if newNode.HighRangeBin.To16() == nil {
				return list, errors.New("High range IP invalid")
			}
		}
		list = append(list, newNode)
	}
	return list, nil
}
