package search

import (
	"bytes"
	"errors"
	"log"
	"net"
	"encoding/binary"

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
func plusOne(a net.IP) net.IP {
	a = append([]byte(nil), a...)
	var i int
	for i = 15; a[i] == 255; i-- {
		a[i] = 0
	}
	a[i]++
	return a
}
func FindMiddle(low,high net.IP) net.IP {
	lowInt := ip2int(low) 
	highInt := ip2int(high)
	middleInt := int((highInt - lowInt)/2)
	//return int2ip(middleInt)*/
	log.Println(middleInt)
	mid := low
	i := 0
	if middleInt < 500 {
		for i < middleInt/2 {
			mid = plusOne(mid) 
			i++
		}
	}
	return mid
}
func ip2int(ip net.IP) uint32 {
	if len(ip) == 16 {
		return binary.BigEndian.Uint32(ip[12:16])
	}
	return binary.BigEndian.Uint32(ip)
}
func SearchBinary(list []parser.IPNode, ipLookUp string) (p parser.IPNode, e error) {
	start := 0
	end := len(list) - 1

	userIP := net.ParseIP(ipLookUp)
	for start <= end {
		median := (start + end) / 2
		if bytes.Compare(userIP, list[median].IPAddressLow) >= 0 && bytes.Compare(userIP, list[median].IPAddressHigh) <= 0 {
			// When in the correct neighborhood of nested IP's call linear search
			return searchLinear(list, ipLookUp, median)
		}
		if bytes.Compare(userIP, list[median].IPAddressLow) > 0 {
			start = median + 1
		} else {
			end = median - 1
		}
	}
	return p, errors.New("Node not found\n")
}

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

