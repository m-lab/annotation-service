package annotator

import (
	"cloud.google.com/go/storage"
	"encoding/csv"
	"errors"
	"fmt"
//	"google.golang.org/appengine"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"golang.org/x/net/context"
	//storage_v1 "google.golang.org/api/storage/v1"
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
func lookupAndRespond(r *http.Request, w http.ResponseWriter, ip string, time_milli int64) {
	n, err := search(w, r, ip)
	if err != nil {
		fmt.Fprintf(w, "ERROR, IP ADDRESS NOT FOUND\n")
	} else {
		fmt.Fprintf(w, "time: %d \n[\n  {\"ip\": \"%s\", \"type\": \"STRING\"},\n  {\"country\": \"%s\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"%s\", \"type\": \"STRING\"},\n]", time_milli, ip, n.countryName, n.countryAbrv)
	}
}

// creates a list with given Geo IP Country csv file.
// converts parameter (given in bnary IP address) to a decimal
func search(w http.ResponseWriter, r *http.Request, ipLookUp string) (*Node, error) {
	list, err := createList(w, r)
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
func createList(write http.ResponseWriter, request *http.Request) ([]Node, error) {

	list := []Node{}

	//ctx := appengine.NewContext(request)
	//client, err := storage.NewClient(ctx)
	ctx := context.Background() 
	client, err := storage.NewClient(ctx)


	if err != nil {
		log.Fatal(err)
	}

	bkt := client.Bucket("test-annotator-sandbox")

	obj := bkt.Object("annotator-data/GeoIPCountryWhois.csv")
	reader, err := obj.NewReader(ctx)

	if err != nil {
		log.Fatal(err)
	}

	/*if _, err := io.Copy(write, reader); err != nil {
		log.Fatal(err)
	} */

	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}

		var newNode Node

		for value := range record {
			//GO enum version of this?
			if value == 0 {
				newNode.lowRangeBin = record[value]
			} else if value == 1 {
				newNode.highRangeBin = record[value]
			} else if value == 2 {
				temp, err := strconv.Atoi(record[value])
				if err != nil {
					break
				}
				newNode.lowRangeNum = temp
			} else if value == 3 {
				temp, err := strconv.Atoi(record[value])
				if err != nil {
					break
				}
				newNode.highRangeNum = temp
			} else if value == 4 {
				newNode.countryAbrv = record[value]
			} else if value == 5 {
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
