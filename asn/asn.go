package asn

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/metrics"
)

var (
	expectedColumnCount             = 3                                                              // the number of the expected columns in the source dataset
	maxErrorCountPerFile            = 50                                                             // the maximum allowed error per import file
	timeComponentsFromFileNameRegex = regexp.MustCompile(`.*(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2}).*`) // the regex, which helps to extract the time from the file name
)

// ASNDataset holds the database in the memory
type ASNDataset struct {
	IPList []ASNIPNode
	Start  time.Time // Date from which to start using this dataset
}

// ASNIPNode represents a single line in the source dataset file.
type ASNIPNode struct {
	IPAddressLow  net.IP
	IPAddressHigh net.IP
	ASNString     string
}

var lastLogTime = time.Time{}

// Annotate expects an IP string and an api.GeoData pointer to find the ASN
// and populate the data into the GeoData.ASN struct
func (asn *ASNDataset) Annotate(ip string, ann *api.GeoData) error {
	if asn == nil {
		return errors.New("ErrNilASNDataset") // TODO
	}
	if ann.ASN != nil {
		return errors.New("ErrAlreadyPopulated") // TODO
	}
	node, err := asn.SearchBinary(ip)
	if err != nil {
		// ErrNodeNotFound is super spammy - 10% of requests, so suppress those.
		if err != geolite2.ErrNodeNotFound {
			// Horribly noisy now.
			if time.Since(lastLogTime) > time.Minute {
				log.Println(err, ip)
				lastLogTime = time.Now()
			}
		}
		//TODO metric here
		return err
	}

	result := []api.ASNElement{}
	for _, asn := range strings.Split(node.ASNString, ",") {
		asnList := strings.Split(asn, "_")
		newElement := api.ASNElement{ASNList: asnList, ASNListType: api.ASNSingle}
		if len(asnList) > 1 {
			newElement.ASNListType = api.ASNMultiOrigin
		}
		result = append(result, newElement)
	}
	ann.ASN = result
	return nil
}

// The date associated with the dataset.
func (asn *ASNDataset) AnnotatorDate() time.Time {
	return asn.Start
}

// LoadASNDataset loads a dataset from a GCS object.
func LoadASNDataset(file *storage.ObjectAttrs) (api.Annotator, error) {
	dataFileName := loader.GetGzBase(file.Name)
	err := loader.UncompressGzFile(context.Background(), file.Bucket, file.Name, dataFileName)
	if err != nil {
		return nil, err
	}
	defer os.Remove(dataFileName)
	nodes, err := loadData(dataFileName, file.Name)
	if err != nil {
		return nil, err
	}

	time, err := ExtractTimeFromASNFileName(dataFileName)
	if err != nil {
		return nil, err
	}
	return &ASNDataset{IPList: nodes, Start: *time}, nil
}

// ExtractTimeFromASNFileName extract the start time of the dataset validity
// from the name of the file.
func ExtractTimeFromASNFileName(fileName string) (*time.Time, error) {
	groups := timeComponentsFromFileNameRegex.FindStringSubmatch(fileName)
	if groups == nil {
		log.Printf("Could not extract time from ASN filename: %s\n", fileName)
		return nil, errors.New("cannot extract date from input filename")
	}

	// We can be sure that we have integers in the groups, otherwise the regexp
	// wouldn't match, so we cast those to int without error check
	// Based on the docs, the time should be ment in UTC
	t := time.Date(
		asIntUnsafe(groups[1]),
		time.Month(asIntUnsafe(groups[2])),
		asIntUnsafe(groups[3]),
		asIntUnsafe(groups[4]),
		asIntUnsafe(groups[5]),
		0,
		0,
		time.UTC)

	return &t, nil
}

// asIntUnsafe converts the string to int without error check.
// the caller should be sure about the passed string can be converted
// to int
func asIntUnsafe(knownIntStr string) int {
	v, _ := strconv.Atoi(knownIntStr)
	return v
}

// loadData loads the data into an ASNIPNode list
// FIXME eliminate, it's a copy-paste of LoadIPListGLite2 from geo-g2.go
func loadData(fileName, datasetName string) ([]ASNIPNode, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.FieldsPerRecord = expectedColumnCount

	list := []ASNIPNode{}
	stack := []ASNIPNode{}

	errorCount := 0
	maxErrorCount := maxErrorCountPerFile
	for {
		var newNode ASNIPNode
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, errors.New("Too many errors during loading the dataset IP list.")
			}
			continue
		}
		ipString := strings.Join(record[:2], "/")
		lowIP, highIP, err := rangeCIDR(ipString)
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, errors.New("Too many errors during loading the dataset IP list")
			}
			continue
		}
		newNode.IPAddressLow = lowIP
		newNode.IPAddressHigh = highIP
		newNode.ASNString = record[2]

		stack, list = handleStack(stack, list, newNode)
	}
	var pop ASNIPNode
	pop, stack = stack[len(stack)-1], stack[:len(stack)-1]
	for ; len(stack) > 0; pop, stack = stack[len(stack)-1], stack[:len(stack)-1] {
		peek := stack[len(stack)-1]
		peek.IPAddressLow = PlusOne(pop.IPAddressHigh)
		list = append(list, peek)
	}
	return list, nil
}

// Finds the smallest and largest net.IP from a CIDR range
// Example: "1.0.0.0/24" -> 1.0.0.0 , 1.0.0.255
// FIXME eliminate, it's a copy-paste of rangeCIDR from geo-g2.go
func rangeCIDR(cidr string) (net.IP, net.IP, error) {
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, nil, errors.New("Invalid CIDR IP range")
	}
	lowIP := make(net.IP, len(ip))
	copy(lowIP, ip)
	mask := ipnet.Mask
	for x := range ip {
		if len(mask) == 4 {
			if x < 12 {
				ip[x] |= 0
			} else {
				ip[x] |= ^mask[x-12]
			}
		} else {
			ip[x] |= ^mask[x]
		}
	}
	return lowIP, ip, nil
}

// TODO(gfr) What are list and stack?
// handleStack finds the proper place in the stack for the new node.
// `stack` holds a stack of nested IP ranges not yet resolved.
// `list` is the complete list of flattened IPNodes.
// FIXME eliminate, it's a copy-paste of handleStqck from geo-ip.go
func handleStack(stack, list []ASNIPNode, newNode ASNIPNode) ([]ASNIPNode, []ASNIPNode) {
	// Stack is not empty aka we're in a nested IP
	if len(stack) != 0 {
		// newNode is no longer inside stack's nested IP's
		if lessThan(stack[len(stack)-1].IPAddressHigh, newNode.IPAddressLow) {
			// while closing nested IP's
			var pop ASNIPNode
			pop, stack = stack[len(stack)-1], stack[:len(stack)-1]
			for ; len(stack) > 0; pop, stack = stack[len(stack)-1], stack[:len(stack)-1] {
				peek := stack[len(stack)-1]
				if lessThan(newNode.IPAddressLow, peek.IPAddressHigh) {
					// if there's a gap in between adjacent nested IP's,
					// complete the gap
					peek.IPAddressLow = PlusOne(pop.IPAddressHigh)
					peek.IPAddressHigh = minusOne(newNode.IPAddressLow)
					list = append(list, peek)
					break
				}
				peek.IPAddressLow = PlusOne(pop.IPAddressHigh)
				list = append(list, peek)
			}
		} else {
			// if we're nesting IP's
			// create begnning bounds
			lastListNode := &list[len(list)-1]
			lastListNode.IPAddressHigh = minusOne(newNode.IPAddressLow)

		}
	}
	stack = append(stack, newNode)
	list = append(list, newNode)
	return stack, list
}

// lessThan returns true if the net.IP in the first argument is lower than
// the net.IP in the second argument
// FIXME eliminate, it's a copy-paste of lessThan from geo-ip.go
func lessThan(a, b net.IP) bool {
	return bytes.Compare(a, b) < 0
}

// PlusOne adds one to a net.IP.
// FIXME eliminate, it's a copy-paste of PlusOne from geo-ip.go
func PlusOne(a net.IP) net.IP {
	a = append([]byte(nil), a...)
	var i int
	for i = 15; a[i] == 255; i-- {
		a[i] = 0
	}
	a[i]++
	return a
}

// minusOne subtracts one of a net.IP
// FIXME eliminate, it's a copy-paste of minusOne from geo-ip.go
func minusOne(a net.IP) net.IP {
	a = append([]byte(nil), a...)
	var i int
	for i = 15; a[i] == 0; i-- {
		a[i] = 255
	}
	a[i]--
	return a
}

// SearchBinary does a binary search for a list element.
// FIXME eliminate, it's a copy-paste of BinarySearch from geo-ip.go
func (ds *ASNDataset) SearchBinary(ipLookUp string) (p ASNIPNode, e error) {
	ip := net.ParseIP(ipLookUp)
	if ip == nil {
		metrics.BadIPTotal.Inc()
		return p, errors.New("ErrInvalidIP") // TODO
	}
	list := ds.IPList
	start := 0
	end := len(list) - 1

	for start <= end {
		median := (start + end) / 2
		if bytes.Compare(ip, list[median].IPAddressLow) >= 0 && bytes.Compare(ip, list[median].IPAddressHigh) <= 0 {
			return list[median], nil
		}
		if bytes.Compare(ip, list[median].IPAddressLow) > 0 {
			start = median + 1
		} else {
			end = median - 1
		}
	}
	return p, geolite2.ErrNodeNotFound
}
