package iputils

import (
	"bytes"
	"encoding/csv"
	"errors"
	"io"
	"net"

	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/metrics"
)

var (
	// the maximum number of wrong records per file during the import
	maxWrongRecordsPerFile = 50

	// ErrorTooManyErrors raised when the maximum number of errors during the import of a single file is > then maxWrongRecordsPerFile
	ErrorTooManyErrors = errors.New("Too many errors during loading the dataset IP list")

	// ErrNodeNotFound raised when a node is not found during SearchBinary
	ErrNodeNotFound = errors.New("node not found")

	// ErrEmptyIP is returned for IP address strings that are empty.
	ErrEmptyIP = errors.New("Empty IP address")
	// ErrInvalidIP is returned for non-empty IP address strings that cannot be parsed.
	ErrInvalidIP = errors.New("Invalid IP address")
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
	DataEquals(other IPNode) bool     // should return true if the data EXCEPT the IP adresses match
}

// IPNodeParser interface enables the abstraction of processing the source data with the common logic.
// This interface should be implemented for all the IP range based datasources (currently GeoLite2 and RouteView ASNs)
type IPNodeParser interface {
	PreconfigureReader(reader *csv.Reader) error           // should customize the CSV reader to the datasource-specific format
	ValidateRecord(record []string) error                  // should validate the raw CSV record
	ExtractIP(record []string) string                      // should extract the IP (with bitmask) from a single raw CSV record
	PopulateRecordData(record []string, node IPNode) error // should parse the raw CSV record and populate the necessary data to the IPNode
	CreateNode() IPNode                                    // should create a new instance of datasource-specific IPNode

	// KZ: These functions needs to be implemented because the collection of the list items should be handled by the IPNodeParser.
	// This could be handled generally by iputils package as well by using interfaces, however on this scale this would have a significant
	// memory penalty because of the interface vs struct memory layout. To keep the common logic in one place and externalize
	// the storage of the created list item to the datasource-specific IPNodeParser the logic below should be implemented by
	// the datasource-specific IPNodeParser
	AppendNode(node IPNode) // should append a datasource-specific IPNode to the list
	LastNode() IPNode       // should return the last node.
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

// ParseIPWithMetrics parses an IP address string, returning a net.IP, or parse error.
func ParseIPWithMetrics(ip string) (net.IP, error) {
	if ip == "" {
		metrics.BadIPTotal.WithLabelValues("Empty").Inc()
		return nil, ErrEmptyIP
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		metrics.BadIPTotal.WithLabelValues("Invalid").Inc()
		return nil, ErrInvalidIP
	}

	return parsed, nil
}

// SearchBinary does a binary search for a list element in the specified list
func SearchBinary(ip net.IP, nodeListSize int, ipNodeGetter func(idx int) IPNode) (p IPNode, e error) {
	start := 0
	end := nodeListSize - 1

	for start <= end {
		median := (start + end) / 2
		medianNode := ipNodeGetter(median)
		if bytes.Compare(ip, medianNode.GetLowIP()) >= 0 && bytes.Compare(ip, medianNode.GetHighIP()) <= 0 {
			return medianNode, nil
		}
		if bytes.Compare(ip, medianNode.GetLowIP()) > 0 {
			start = median + 1
		} else {
			end = median - 1
		}
	}
	return p, ErrNodeNotFound
}

// ipNodeConsumer is the consumer for loader.CSVReader
type ipNodeCSVConsumer struct {
	parser IPNodeParser
	stack  []IPNode
}

// factory method for ipNodeCSVConsumer
func newIPNodeCSVConsumer(parser IPNodeParser) *ipNodeCSVConsumer {
	return &ipNodeCSVConsumer{
		parser: parser,
		stack:  []IPNode{},
	}
}

// PreconfigureReader just propagates to the IPNodeParser
func (c *ipNodeCSVConsumer) PreconfigureReader(reader *csv.Reader) error {
	return c.parser.PreconfigureReader(reader)
}

// ValidateRecord just propagates to the IPNodeParser
func (c *ipNodeCSVConsumer) ValidateRecord(record []string) error {
	return c.parser.ValidateRecord(record)
}

// Consume implements the IPList building logic
func (c *ipNodeCSVConsumer) Consume(record []string) error {
	lowIP, highIP, err := rangeCIDR(c.parser.ExtractIP(record))
	if err != nil {
		return err
	}
	newNode := c.parser.CreateNode()
	newNode.SetIPBounds(lowIP, highIP)
	err = c.parser.PopulateRecordData(record, newNode)
	if err != nil {
		return err
	}

	// merge if it's possible
	lastNode := c.parser.LastNode()
	if lastNode != nil && canBeMergedByIP(lastNode, newNode) && lastNode.DataEquals(newNode) {
		// we can merge, so if the new node's IP is greater, that will be the new high IP of the last node
		if lessThan(lastNode.GetHighIP(), newNode.GetHighIP()) {
			lastNode.SetHighIP(newNode.GetHighIP())
		}
		return nil
	}

	c.stack = handleStack(c.stack, c.parser, newNode)
	return nil
}

func canBeMergedByIP(prev, next IPNode) bool {
	nextLowIP := minusOne(next.GetLowIP())
	return bytes.Compare(prev.GetHighIP(), nextLowIP) >= 0 // equals or the next low is lower than the prev high
}

// BuildIPNodeList is a modified version of geolite2.LoadIPListGLite2 implementation. Uses the same logic
// but performs the datasource-specific operations through the parser passed in the parameter
func BuildIPNodeList(reader io.Reader, parser IPNodeParser) error {
	consumer := newIPNodeCSVConsumer(parser)
	csvReader := loader.NewCSVReader(reader, consumer)
	err := csvReader.ReadAll()
	if err != nil {
		return err
	}
	finalizeStackAndList(consumer.stack, parser)
	return nil
}

// finalizeStackAndList processes the remaining elements on the stack and closes the list
// if it's necessary (if a parent range should have a subrange after the last embedded range)
func finalizeStackAndList(stack []IPNode, parser IPNodeParser) []IPNode {
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
		parser.AppendNode(peek)
	}
	return stack
}

// handleStack works the same way as it worked in geolite2 package. The only difference is that
// it uses the IPNode interface's getters and setters for abstraction.
//
// TODO(gfr) What are list and stack?
// handleStack finds the proper place in the stack for the new node.
// `stack` holds a stack of nested IP ranges not yet resolved.
// `list` is the complete list of flattened IPNodes.
func handleStack(stack []IPNode, parser IPNodeParser, newNode IPNode) []IPNode {
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
					parser.AppendNode(peekCpy)
					break
				}
				peekCpy.SetLowIP(plusOne(pop.GetHighIP()))
				parser.AppendNode(peekCpy)
			}
		} else {
			// if we're nesting IP's
			// create begnning bounds
			lastListNode := parser.LastNode()
			lastListNode.SetHighIP(minusOne(newNode.GetLowIP()))

		}
	}
	stack = append(stack, newNode)

	// items in stack should not change, so we store the copy of the original items in stack
	parser.AppendNode(newNode.Clone())
	return stack
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
