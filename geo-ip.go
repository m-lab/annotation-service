package annotator

import (
	"cloud.google.com/go/storage"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"golang.org/x/net/context"
	"fmt"
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
func lookupAndRespond(bucket string, bucketObj string,r *http.Request, w http.ResponseWriter, ip string, time_milli int64) {
	n, err := search(bucket, bucketObj, w, r, ip)
	if err != nil {
		fmt.Fprintf(w, "ERROR, IP ADDRESS NOT FOUND\n")
	} else {
		fmt.Fprintf(w, "time: %d \n[\n  {\"ip\": \"%s\", \"type\": \"STRING\"},\n  {\"country\": \"%s\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"%s\", \"type\": \"STRING\"},\n]", time_milli, ip, n.countryName, n.countryAbrv)
	}
}

// creates a list with given Geo IP Country csv file.
// converts parameter (given in bnary IP address) to a decimal
func search(bucket string, bucketObj string, w http.ResponseWriter, r *http.Request, ipLookUp string) (*Node, error) {
	list, err := createList(bucket, bucketObj, w, r)
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

//reads in CSV file into an array to be searched through
func createList(bucket string, bucketObj string, write http.ResponseWriter, request *http.Request) ([]Node, error) {

	list := []Node{}

	//ctx := appengine.NewContext(request)
	//client, err := storage.NewClient(ctx)
	ctx := context.Background() 
	client, err := storage.NewClient(ctx)


	if err != nil {
		log.Fatal(err)
	}
// make parameter / 
	bkt := client.Bucket(bucket)

	obj := bkt.Object(bucketObj)
	reader, err := obj.NewReader(ctx)

	if err != nil {
		log.Fatal(err)
	}

	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}

		var newNode Node

		for value := range record {
			switch value{
				case 0: 
					newNode.lowRangeBin = record[value]
				case 1: 
					newNode.highRangeBin = record[value]
				case 2: 
					temp, err := strconv.Atoi(record[value])
					if err != nil {
						break
					}
					newNode.lowRangeNum = temp
				case 3: 
	                                temp, err := strconv.Atoi(record[value])
	                                if err != nil {
	                                   break
					}  
					newNode.highRangeNum = temp
				case 4:
					newNode.countryAbrv = record[value]
				case 5: 
					newNode.countryName = record[value]
					list = append(list, newNode)
			}
		}

	}
	return list, nil
}

// searches through array containing CSV file contents
func searchList(list []Node, userIp int) (*Node, error) {
	for value := range list {
		if userIp >= list[value].lowRangeNum && userIp <= list[value].highRangeNum {
			return &list[value], nil
		}
	}
	return nil, errors.New("not found\n")
}
