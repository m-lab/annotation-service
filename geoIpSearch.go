package annotator

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
)

type Node struct {
	lowRangeBin  string
	highRangeBin string
	lowRangeNum  int
	highRangeNum int
	countryAbrv  string
	countryName  string
}

func lookupAndRespond(w http.ResponseWriter, ip string, time_milli int64) {
	n := search(ip, w)
	fmt.Fprintf(w,
		"time: %d \n[\n  {\"ip\": \"%s\", \"type\": \"STRING\"},\n  {\"country\": \"%s\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"%s\", \"type\": \"STRING\"},\n]", time_milli, ip, n.countryName, n.countryAbrv)
}

func search(ipLookUp string, w http.ResponseWriter) Node {
	list := createList(w)
	return searchList(list, ipLookUp)
}


func createList(w http.ResponseWriter) []Node {
	list := []Node{}

	if _, err := os.Stat("GeoIPCountryWhois.csv"); os.IsNotExist(err) {
		fmt.Fprintf(w, "data file not found\n")
	}

	f, _ := os.Open("GeoIPCountryWhois.csv")

	r := csv.NewReader(bufio.NewReader(f))
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

	return list
}
func searchList(list []Node, ipLookUp string) Node {

	userIp, err := strconv.Atoi(ipLookUp)
	if err != nil {
		panic(err)
	}
	for value := range list {
		if userIp >= list[value].lowRangeNum && userIp <= list[value].highRangeNum {
			return list[value]
		}
	}
	panic(err)
}
