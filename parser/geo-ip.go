package parser

//Reads in CSV file and creates a node list

import (
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

//Reads file from given reader and creates a node list
func CreateListIPv4(reader io.Reader) ([]Node, error) {
	list := []Node{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		var newNode Node
		newNode.LowRangeBin = net.ParseIP(record[0])
		newNode.HighRangeBin = net.ParseIP(record[1])
		newNode.CountryAbrv = record[4]
		newNode.CountryName = record[5]
		list = append(list, newNode)

	}
	return list, nil
}

func CreateListIPv6(reader io.Reader) ([]Node, error) {
	list := []Node{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		var newNode Node
		newNode.LowRangeBin = net.ParseIP(record[0])
		newNode.HighRangeBin = net.ParseIP(record[1])
		newNode.CountryAbrv = record[4]
		newNode.CountryName = "N/A"
		list = append(list, newNode)

	}
	return list, nil
}
