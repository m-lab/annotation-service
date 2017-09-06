package parser

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"math"
	"net"
	"reflect"
	"strconv"
)

const (
	ipNumColumnsGlite1       = 3
	locationNumColumnsGlite1 = 9
	gLite1Prefix             = "GeoLiteCity"
)

// Glite1HelpNode defines IPNode data defined inside
// GeoLite1 Location files
type gLite1HelpNode struct {
	Latitude   float64
	Longitude  float64
	PostalCode string
}

// TODO: Add equivalent of LoadGeoLite2

// Create Location list, map, and Glite1HelpNode for GLite1 databases
// GLiteHelpNode contains information to help populate fields in IPNode
func LoadLocListGLite1(reader io.Reader) ([]LocationNode, []gLite1HelpNode, map[int]int, error) {
	r := csv.NewReader(reader)
	idMap := make(map[int]int, mapMax)
	list := []LocationNode{}
	glite := []gLite1HelpNode{}
	// Skip the first 2 lines
	_, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, nil, nil, errors.New("Empty input data")
	}
	_, err = r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, nil, nil, errors.New("Empty input data")
	}
	r.FieldsPerRecord = locationNumColumnsGlite1
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else if len(record) != r.FieldsPerRecord {
				log.Println(err)
				log.Println("\twanted: ", locationNumColumnsGlite1, " got: ", len(record), record)
				return nil, nil, nil, errors.New("Corrupted Data: wrong number of columns")
			} else {
				log.Println(err, ": ", record)
				return nil, nil, nil, errors.New("Error reading file")
			}
		}
		var lNode LocationNode
		lNode.GeonameID, err = strconv.Atoi(record[0])
		if err != nil {
			return nil, nil, nil, errors.New("Corrupted Data: GeonameID should be a number")
		}
		lNode.CountryCode, err = checkCaps(record[1], "Country code")
		if err != nil {
			return nil, nil, nil, err
		}
		lNode.CityName = record[3]
		var gNode gLite1HelpNode
		gNode.PostalCode = record[4]
		gNode.Latitude, err = stringToFloat(record[5], "Latitude")
		if err != nil {
			return nil, nil, nil, err
		}
		gNode.Longitude, err = stringToFloat(record[6], "Longitude")
		if err != nil {
			return nil, nil, nil, err
		}

		list = append(list, lNode)
		glite = append(glite, gNode)
		idMap[lNode.GeonameID] = len(list) - 1
	}
	return list, glite, idMap, nil
}

// Creates a List of IPNodes
func LoadIPListGLite1(reader io.Reader, idMap map[int]int, glite1 []gLite1HelpNode) ([]IPNode, error) {
	g1IP := []string{"startIpNum", "endIpNum", "locId"}
	list := []IPNode{}
	stack := []IPNode{}
	r := csv.NewReader(reader)
	// Skip first line
	title, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, errors.New("Empty input data")
	}
	// Skip 2nd line, which contains column labels
	title, err = r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, errors.New("Empty input data")
	}
	if !reflect.DeepEqual(g1IP, title) {
		log.Println("Improper data format got: ", title, " wanted: ", g1IP)
		return nil, errors.New("Improper data format")
	}
	var newNode IPNode

	for {
		// Example:
		// GLite1 : record = [16777216,16777471,17]
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		err = checkNumColumns(record, ipNumColumnsGlite1)
		if err != nil {
			return nil, err
		}
		newNode.IPAddressLow, err = intToIPv4(record[0])
		if err != nil {
			return nil, err
		}
		newNode.IPAddressHigh, err = intToIPv4(record[1])
		if err != nil {
			return nil, err
		}
		// Look for GeoId within idMap and return index
		index, err := lookupGeoId(record[2], idMap)
		if err != nil {
			return nil, err
		}
		newNode.LocationIndex = index
		newNode.Latitude = glite1[index].Latitude
		newNode.Longitude = glite1[index].Longitude
		newNode.PostalCode = glite1[index].PostalCode
		// Stack is not empty aka we're in a nested IP
		if len(stack) != 0 {
			//log.Println("here")
			// newNode is no longer inside stack's nested IP's
			if lessThan(stack[len(stack)-1].IPAddressHigh, newNode.IPAddressLow) {
				// while closing nested IP's
				//log.Println("HE-------______--____RE")
				for len(stack) > 0 {
					var pop IPNode
					//log.Println("forloop",stack)
					pop, stack = stack[len(stack)-1], stack[:len(stack)-1]
					if len(stack) == 0 {
						break
					}
					peek := stack[len(stack)-1]
					if lessThan(newNode.IPAddressLow, peek.IPAddressHigh) {
						// if theres a gap inbetween imediately nested IP's
						if len(stack) > 0 {
							//log.Println("current stack: ",stack)
							//complete the gap
							log.Println("before: ", peek)
							peek.IPAddressLow = addOne(pop.IPAddressHigh)
							peek.IPAddressHigh = deleteOne(newNode.IPAddressLow)
							log.Println("after: ", peek)
							list = append(list, peek)
						}
						break
					}
					peek.IPAddressLow = addOne(pop.IPAddressHigh)
					list = append(list, peek)
				}
			} else {
				// if we're nesting IP's
				// create begnning bounds
				lastListNode := &list[len(list)-1]
				log.Println("BEFORE: ", lastListNode.IPAddressLow, "-----", newNode.IPAddressLow)
				lastListNode.IPAddressHigh = deleteOne(newNode.IPAddressLow)
				log.Println("AFTER: ", lastListNode.IPAddressLow, "-----", lastListNode.IPAddressHigh)

			}
		}
		stack = append(stack, newNode)
		list = append(list, newNode)
		log.Println("LIST: ", list)
		newNode.IPAddressLow = newNode.IPAddressHigh
		newNode.IPAddressHigh = net.IPv4(255, 255, 255, 255)

	}
	log.Println(stack)
	for len(stack) > 0 {
		var pop IPNode
		pop, stack = stack[len(stack)-1], stack[:len(stack)-1]
		if len(stack) == 0 {
			break
		}
		peek := stack[len(stack)-1]
		peek.IPAddressLow = addOne(pop.IPAddressHigh)
		list = append(list, peek)
	}
	log.Println("LIST: ", list)
	return list, nil
}

func addOne(a net.IP) net.IP {
	a = append([]byte(nil), a...)
	var i int
	for i = 15; a[i] == 255; i-- {
		a[i] = 0
	}
	a[i]++
	return a
}
func deleteOne(a net.IP) net.IP {
	a = append([]byte(nil), a...)
	var i int
	for i = 15; a[i] == 0; i-- {
		a[i] = 255
	}
	a[i]--
	return a
}
func moreThan(a, b net.IP) bool {
	return bytes.Compare(a, b) > 0
}
func lessThan(a, b net.IP) bool {
	return bytes.Compare(a, b) < 0
}

// Converts integer to net.IPv4
func intToIPv4(str string) (net.IP, error) {
	num, err := strconv.ParseInt(str, 10, 0)
	if err != nil {
		log.Println("Provided IP should be a number")
		return nil, errors.New("Inputed string cannot be converted to a number")
	}
	// TODO: get rid of floating point
	ft := float64(num)
	if ft > math.Pow(2, 32) || num < 0 {
		log.Println("Provided IP should be in the range of 0.0.0.1 and 255.255.255.255 ", str)
	}
	ip := make(net.IP, 4)
	// Split number into array of bytes
	binary.BigEndian.PutUint32(ip, uint32(num))
	ip = net.IPv4(ip[0], ip[1], ip[2], ip[3])
	return ip, nil
}
