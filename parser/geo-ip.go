//Depending on whether user input was an IPv4 or IPv6 IPaddress,
//respective database file will be read in and a list of Nodes will be created
//Each node contains a geo-location and its range of IP addresses
package parser

import (
	"errors"
	"io"
	"net"

	"encoding/csv"
)

// Node defines the range of IP addresses per country
type Node struct {
	// Low range binary
	LowRangeBin net.IP
	// High range binary
	HighRangeBin net.IP
	// Country abreviation
	CountryAbrv string
	// Country name
	CountryName string
}

func NewNode(lrb, hrb net.IP, ctryA, ctryN string) Node {
	return Node{lrb, hrb, ctryA, ctryN}
}

//Creates a List of nodes for either IPv4 or IPv6 databases.
func CreateList(reader io.Reader, IPVersion int) ([]Node, error) {
	list := []Node{}
	unzip, err := gzip.NewReader(reader) 
	if err != nil {
		return list, errors.New("unzipping didn't work") 
	}
	defer unzip.Close()
	r := csv.NewReader(unzip)
	r.TrimLeadingSpace = true
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
