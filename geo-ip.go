package annotator

//Reads in CSV file and creates a node list

import (
	"encoding/csv"
	"io"
	"strconv"
)

// Node defines the range of IP addresses per country
type Node struct {
	// Low range binary
	lowRangeBin string
	// High range binary
	highRangeBin string
	// Low range dec
	lowRangeNum int
	// High range dec
	highRangeNum int
	// Country abreviation
	countryAbrv string
	// Country name
	countryName string
}

//Reads file from given reader and creates a node list
func createList(reader io.Reader) ([]Node, error) {
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
		newNode.lowRangeBin = record[0]
		newNode.highRangeBin = record[1]
		binaryString, err := strconv.Atoi(record[2])
		if err != nil {
			break
		}
		newNode.lowRangeNum = binaryString
		binaryString2, err := strconv.Atoi(record[3])
		if err != nil {
			break
		}
		newNode.highRangeNum = binaryString2
		newNode.countryAbrv = record[4]
		newNode.countryName = record[5]
		list = append(list, newNode)

	}
	return list, nil
}
