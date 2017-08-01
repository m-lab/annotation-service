package annotator

import (
	"cloud.google.com/go/storage"
	"encoding/csv"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
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

// searches for country codes with search func, and replies to http responder
func lookupAndRespond(list []Node, w http.ResponseWriter, ip string) {

	n, err := search(list, ip)
	if err != nil {
		fmt.Fprintf(w, "ERROR, IP ADDRESS NOT FOUND\n")
	} else {
		fmt.Fprintf(w, "[\n  {\"ip\": \"%s\", \"type\": \"STRING\"},\n  {\"country\": \"%s\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"%s\", \"type\": \"STRING\"},\n]", ip, n.countryName, n.countryAbrv)
	}
}

// creates a list with given Geo IP Country csv file.
// converts parameter (given in bnary IP address) to a decimal
func search(list []Node, ipLookUp string) (*Node, error) {
	if err != nil {
		return nil, err
	}
	ipDecimal, err := bin2Dec(ipLookUp)
	if err != nil {
		return nil, err
	}
	n, err := searchList(list, ipDecimal)
	if err != nil {
		return nil, err
	}
	return n, nil
}

//converts binary IP address to decimal form. used for search
func bin2Dec(ipLookUp string) (int, error) {
	n := strings.Split(ipLookUp, ".")
	m := []int{}

	for _, i := range n {

		//error handling is done in the caller
		j, err := strconv.Atoi(i)
		if err != nil {
			return 0, err
		}

		m = append(m, j)
	}
	return (m[0] << 24) + (m[1] << 16) + (m[2] << 8) + m[3], nil
}

//creates generic reader
func createReader(bucket string, bucketObj string, ctx context.Context) (*storage.Reader, error) {

	client, err := storage.NewClient(ctx)

	if err != nil {
		log.Fatal(err)
	}

	bkt := client.Bucket(bucket)

	obj := bkt.Object(bucketObj)
	reader, err := obj.NewReader(ctx)

	if err != nil {
		log.Fatal(err)
	}
	return reader, nil

}

// request isnt needed - only reader is needed for parameter
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
		temp, err := strconv.Atoi(record[2])
		if err != nil {
			break
		}
		newNode.lowRangeNum = temp

		temp2, err := strconv.Atoi(record[3])
		if err != nil {
			break
		}

		newNode.highRangeNum = temp2

		newNode.countryAbrv = record[4]

		newNode.countryName = record[5]

		list = append(list, newNode)

	}
	return list, nil
}

// searches through array containing CSV file contents
func searchList(list []Node, userIp int) (*Node, error) {
	for i := range list {
		if userIp >= list[i].lowRangeNum && userIp <= list[i].highRangeNum {
			return &list[i], nil
		}
	}
	return nil, errors.New("not found\n")
}
