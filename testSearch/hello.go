package annotator

import (
	"bufio"
	"encoding/csv"
//	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
//	"strings"
)

const appkey = "Temp Key"

type Node struct {
	lowRangeBin  string
	highRangeBin string
	lowRangeNum  int
	highRangeNum int
	countryAbrv  string
	countryName  string
}

func init() {
	http.HandleFunc("/annotate", annotate)
}

func lookupAndRespond(w http.ResponseWriter, ip string) {
	n := search(ip, w)
	fmt.Fprintf(w,
		"[\n  {\"ip\": \"%s\", \"type\": \"STRING\"},\n  {\"country\": \"%s\", \"type\": \"STRING\"},\n  {\"countryAbrv\": \"%s\", \"type\": \"STRING\"},\n]", ip, n.countryName, n.countryAbrv)
}

func annotate(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	ip := query.Get("ip_addr")
	lookupAndRespond(w, ip)
}
func search(ipLookUp string, w http.ResponseWriter) Node {
	list := createList(w)
	return searchList(list, ipLookUp)
}
/*func newSource(client *http.Client, uri string) {
	if !strings.HasPrefix(uri, "gs://") {
		return nil, errors.New("invalid file path: " + uri)
	}
}*/
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
