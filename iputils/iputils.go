package iputils

import (
	"bytes"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"net"
)

var (
	ErrorTooManyErrors = errors.New("Too many errors during loading the dataset IP list.")
)

// BaseIPNode is a basic type for nodes to handle. This struct should be embedded in all the IP range related
// struct.
type BaseIPNode struct {
	IPAddressLow  net.IP
	IPAddressHigh net.IP
}

// IPNode interface enables the abstraction of different kind of IP nodes to reuse the list building,
// binary search logic. BaseIPNode satisfies this interface except the Clone method, that should be
// implemented for every IP range specific node
type IPNode interface {
	SetIPBounds(lowIP, highIP net.IP) // sets the IP bounds
	GetLowIP() net.IP                 // getter for the lower bound
	SetLowIP(newLow net.IP)           // setter for the lower bound
	GetHighIP() net.IP                // getter for the upper bound
	SetHighIP(newHigh net.IP)         // setter for the upper bound
	Clone() IPNode                    // should create a copy from the node
}

// IPNodeParser interface enables the abstraction of processing the source data with the common logic.
// This interface should be implemented for all the IP range based datasources (currently GeoLite2 and RouteView ASNs)
type IPNodeParser interface {
	PreconfigureReader(reader *csv.Reader) error           // should customize the CSV reader to the datasource-specific format
	NewNode() IPNode                                       // should create a new instance of datasource-specific IPNode
	ValidateRecord(record []string) error                  // should validate the raw CSV record
	ExtractIP(record []string) string                      // should extract the IP (with bitmask) from a single raw CSV record
	PopulateRecordData(record []string, node IPNode) error // should parse the raw CSV record and populate the necessary data to the IPNode
}

// SetIPBounds sets up the bounds of an IPNode
func (n *BaseIPNode) SetIPBounds(lowIP, highIP net.IP) {
	n.IPAddressLow = lowIP
	n.IPAddressHigh = highIP
}

// GetLowIP is a getter for the IPNode's lower bound
func (n *BaseIPNode) GetLowIP() net.IP {
	return n.IPAddressLow
}

// SetLowIP is a setter for the IPNode's lower bound
func (n *BaseIPNode) SetLowIP(newLow net.IP) {
	n.IPAddressLow = newLow
}

// GetHighIP is a getter for the IPNode's upper bound
func (n *BaseIPNode) GetHighIP() net.IP {
	return n.IPAddressHigh
}

// SetHighIP is a setter for the IPNode's upper bound
func (n *BaseIPNode) SetHighIP(newHigh net.IP) {
	n.IPAddressHigh = newHigh
}

// BuildIPNodeList is a modified version of geolite2.LoadIPListGLite2 implementation. Uses exactly the same logic
// but performs the datasource-specific operations through the parser passed in the parameter
func BuildIPNodeList(reader io.Reader, parser IPNodeParser) ([]IPNode, error) {
	r := csv.NewReader(reader)
	err := parser.PreconfigureReader(r)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	list := []IPNode{}
	stack := []IPNode{}

	errorCount := 0
	maxErrorCount := 50
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		err = parser.ValidateRecord(record)
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, ErrorTooManyErrors
			}
			continue
		}
		lowIP, highIP, err := rangeCIDR(parser.ExtractIP(record))
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, ErrorTooManyErrors
			}
			continue
		}
		newNode := parser.NewNode()
		newNode.SetIPBounds(lowIP, highIP)
		err = parser.PopulateRecordData(record, newNode)
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, ErrorTooManyErrors
			}
			continue
		}
		stack, list = handleStack(stack, list, newNode)
	}
	stack, list = finalizeStackAndList(stack, list)
	return list, nil
}

// finalizeStackAndList processes the remaining elements on the stack and closes the list
// if it's necessary (if a parent range should have a subrange after the last embedded range)
func finalizeStackAndList(stack, list []IPNode) ([]IPNode, []IPNode) {
	var pop IPNode
	pop, stack = stack[len(stack)-1], stack[:len(stack)-1]
	for ; len(stack) > 0; pop, stack = stack[len(stack)-1], stack[:len(stack)-1] {
		peek := stack[len(stack)-1]
		peek.SetLowIP(plusOne(pop.GetHighIP()))
		// KZ: there was a bug here (and is in the original implementation as well): when 2 ranges has only intersection
		// and the first one does not contain entirely the next one, a wrong item got appended to the end of the list
		// from the end of the latter range to the end of the prior range. This resulted in IPLow > IPHigh for this item.
		// This might be not be a problem if we can assume that no such intersection can occur in the source datasets.
		// however, here's the fix:
		if lessThan(peek.GetHighIP(), peek.GetLowIP()) {
			continue
		}
		list = append(list, peek)
	}
	return stack, list
}

// handleStack works the same way as it worked in geolite2 package. The only difference is that
// it uses the IPNode interface's getters and setters for abstraction.
//
// TODO(gfr) What are list and stack?
// handleStack finds the proper place in the stack for the new node.
// `stack` holds a stack of nested IP ranges not yet resolved.
// `list` is the complete list of flattened IPNodes.
func handleStack(stack, list []IPNode, newNode IPNode) ([]IPNode, []IPNode) {
	// Stack is not empty aka we're in a nested IP
	if len(stack) != 0 {
		// newNode is no longer inside stack's nested IP's
		if lessThan(stack[len(stack)-1].GetHighIP(), newNode.GetLowIP()) {
			// while closing nested IP's
			var pop IPNode
			pop, stack = stack[len(stack)-1], stack[:len(stack)-1]
			for ; len(stack) > 0; pop, stack = stack[len(stack)-1], stack[:len(stack)-1] {
				peek := stack[len(stack)-1]

				// items in stack should not change, so we're woring on a clone of the element
				peekCpy := peek.Clone()
				if lessThan(newNode.GetLowIP(), peek.GetHighIP()) {
					// if there's a gap in between adjacent nested IP's,
					// complete the gap
					peekCpy.SetLowIP(plusOne(pop.GetHighIP()))
					peekCpy.SetHighIP(minusOne(newNode.GetLowIP()))
					list = append(list, peekCpy)
					break
				}
				peekCpy.SetLowIP(plusOne(pop.GetHighIP()))
				list = append(list, peekCpy)
			}
		} else {
			// if we're nesting IP's
			// create begnning bounds
			lastListNode := list[len(list)-1]
			lastListNode.SetHighIP(minusOne(newNode.GetLowIP()))

		}
	}
	stack = append(stack, newNode)

	// items in stack should not change, so we store the copy of the original items in stack
	list = append(list, newNode.Clone())
	return stack, list
}

// rangeCIDR works the same way as it worked in geolite2 package.
//
// Finds the smallest and largest net.IP from a CIDR range
// Example: "1.0.0.0/24" -> 1.0.0.0 , 1.0.0.255
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

// plusOne adds one to a net.IP.
func plusOne(a net.IP) net.IP {
	a = append([]byte(nil), a...)
	var i int
	for i = 15; a[i] == 255; i-- {
		a[i] = 0
	}
	a[i]++
	return a
}

// minusOne subtracts one from a net.IP.
func minusOne(a net.IP) net.IP {
	a = append([]byte(nil), a...)
	var i int
	for i = 15; a[i] == 0; i-- {
		a[i] = 255
	}
	a[i]--
	return a
}

// lessThan returns true if the net.IP in the first argument is smaller than the net.IP in
// the second argument
func lessThan(a, b net.IP) bool {
	return bytes.Compare(a, b) < 0
}
