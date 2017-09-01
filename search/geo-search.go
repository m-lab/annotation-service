package search

import (
	"bytes"
	"errors"
	"log"
	"net"

	"github.com/m-lab/annotation-service/parser"
)

// Returns a parser.IPNode with the smallet range that includes the provided IP address
func SearchList(list []parser.IPNode, ipLookUp string) (parser.IPNode, error) {
	inRange := false
	var lastNode parser.IPNode
	userIP := net.ParseIP(ipLookUp)
	if userIP == nil {
		log.Println("Inputed IP string could not be parsed to net.IP")
		return lastNode, errors.New("Invalid search IP")
	}
	for _, n := range list {
		if bytes.Compare(userIP, n.IPAddressLow) >= 0 && bytes.Compare(userIP, n.IPAddressHigh) <= 0 {
			inRange = true
			lastNode = n
		} else if inRange && bytes.Compare(userIP, n.IPAddressLow) < 0 {
			return lastNode, nil
		}
	}
	if inRange {
		return lastNode, nil
	}
	return lastNode, errors.New("Node not found\n")
}

// Returns a parser.IPNode with the smallet range that includes the provided IP address
func searchLinear(list []parser.IPNode, ipLookUp string, index int) (parser.IPNode, error) {
	inRange := false
	var lastNode parser.IPNode
	userIP := net.ParseIP(ipLookUp)
	if userIP == nil {
		log.Println("Inputed IP string could not be parsed to net.IP")
		return lastNode, errors.New("Invalid search IP")
	}
	for index < len(list) {
		if bytes.Compare(userIP, list[index].IPAddressLow) >= 0 && bytes.Compare(userIP, list[index].IPAddressHigh) <= 0 {
			inRange = true
			lastNode = list[index]
		} else if inRange && bytes.Compare(userIP, list[index].IPAddressLow) < 0 {
			return lastNode, nil
		}
		index++
	}
	if inRange {
		return lastNode, nil
	}
	return lastNode, errors.New("Node not found\n")
}

func SearchBinary(list []parser.IPNode, ipLookUp string) (p parser.IPNode, e error) {
	start := 0
	end := len(list) - 1

	userIP := net.ParseIP(ipLookUp)
	for start <= end {
		median := (start + end) / 2
		//in range
		if bytes.Compare(userIP, list[median].IPAddressLow) >= 0 && bytes.Compare(userIP, list[median].IPAddressHigh) <= 0 {
			return searchLinear(list, ipLookUp, median)
		}
		//too low
		if bytes.Compare(userIP, list[median].IPAddressLow) > 0 {
			start = median + 1
		} else {
			end = median - 1
		}
	}
	return p, errors.New("Not found")
}
