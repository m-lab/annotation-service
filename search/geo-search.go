package search

import (
	"bytes"
	"errors"
	"log"
	"net"

	"github.com/m-lab/annotation-service/parser"
)

func SearchList(list []parser.IPNode, ipLookUp string) (parser.IPNode, error) {
	inRange := false
	var lastNode parser.IPNode
	userIP := net.ParseIP(ipLookUp)
	if userIP == nil {
		log.Println("Inputed IP string could not be parsed to net.IP")
		return lastNode, errors.New("Ivalid search IP")
	}
	for _, n := range list {
		if bytes.Compare(userIP, n.IPAddressLow) >= 0 && bytes.Compare(userIP, n.IPAddressHigh) <= 0 {
			inRange = true
			lastNode = n
		} else if inRange {
			return lastNode, nil
		}
	}
	if inRange {
		return lastNode, nil
	}
	return lastNode, errors.New("IP not found\n")
}
