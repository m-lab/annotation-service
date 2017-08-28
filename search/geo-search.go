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
