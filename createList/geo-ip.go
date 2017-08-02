package createList

//Reads in CSV file and creates a node list

import (
	"encoding/csv"
	"io"
	"strconv"
)

// Node defines the range of IP addresses per country
type Node struct {
	// Low range binary
	LowRangeBin string
	// High range binary
	HighRangeBin string
	// Low range dec
	LowRangeNum int
	// High range dec
	HighRangeNum int
	// Country abreviation
	CountryAbrv string
	// Country name
	CountryName string
}
func NewNode(lrb,hrb string, lrn,hrn int, ctryA,ctryN string) Node{
	return Node{lrb,hrb,lrn,hrn,ctryA,ctryN}	
}
//Reads file from given reader and creates a node list
func CreateList(reader io.Reader) ([]Node, error) {
	list := []Node{}
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		var newNode Node
		//TODO: scanner instead of individual arguments
		newNode.LowRangeBin = record[0]
		newNode.HighRangeBin = record[1]
		binaryString, err := strconv.Atoi(record[2])
		if err != nil {
			break
		}
		newNode.LowRangeNum = binaryString
		binaryString2, err := strconv.Atoi(record[3])
		if err != nil {
			break
		}
		newNode.HighRangeNum = binaryString2
		newNode.CountryAbrv = record[4]
		newNode.CountryName = record[5]
		list = append(list, newNode)

	}
	return list, nil
}
