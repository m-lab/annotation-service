package geolite2v2

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/m-lab/annotation-service/iputils"
)

var (
	ipNumColumnsGlite2 = 10
)

// GeoIPNode defines IPv4 and IPv6 databases
type GeoIPNode struct {
	iputils.BaseIPNode
	LocationIndex int // Index to slice of locations
	PostalCode    string
	Latitude      float64
	Longitude     float64
}

// Clone clones the GeoIPNode struct to satistfy the IPNode interface
func (n *GeoIPNode) Clone() iputils.IPNode {
	return &GeoIPNode{
		BaseIPNode:    iputils.BaseIPNode{IPAddressLow: n.IPAddressLow, IPAddressHigh: n.IPAddressHigh},
		LocationIndex: n.LocationIndex,
		PostalCode:    n.PostalCode,
		Latitude:      n.Latitude,
		Longitude:     n.Longitude,
	}
}

// DataEquals checks if the data source specific data of the IPNode specified in the parameter is equal to this node. This function
// supports the merge of the equivalent overlapping nodes
func (n *GeoIPNode) DataEquals(other iputils.IPNode) bool {
	otherNode := other.(*GeoIPNode)
	return n.LocationIndex == otherNode.LocationIndex && n.PostalCode == otherNode.PostalCode && n.Latitude == otherNode.Latitude && n.Longitude == otherNode.Longitude
}

// asnNodeParser the parser object
type geoNodeParser struct {
	idMap map[int]int
	list  []GeoIPNode
}

func newGeoNodeParser(locationIDMap map[int]int) *geoNodeParser {
	return &geoNodeParser{
		idMap: locationIDMap,
		list:  []GeoIPNode{},
	}
}

// PreconfigureReader for details see the iputils.IPNodeParser interface!
func (p *geoNodeParser) PreconfigureReader(reader *csv.Reader) error {
	// Skip first line
	_, err := reader.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return errors.New("Empty input data")
	}
	return nil
}

// ValidateRecord for details see the iputils.IPNodeParser interface!
func (p *geoNodeParser) ValidateRecord(record []string) error {
	return checkNumColumns(record, ipNumColumnsGlite2)
}

// ExtractIP for details see the iputils.IPNodeParser interface!
func (p *geoNodeParser) ExtractIP(record []string) string {
	return record[0]
}

// PopulateRecordData for details see the iputils.IPNodeParser interface!
func (p *geoNodeParser) PopulateRecordData(record []string, node iputils.IPNode) error {
	newNode, ok := node.(*GeoIPNode)
	if !ok {
		return errors.New("Illegal node type, expected GeoIPNode")
	}
	// Look for GeoId within idMap and return index
	index, err := lookupGeoID(record[1], p.idMap)
	if err != nil {
		if backupIndex, err := lookupGeoID(record[2], p.idMap); err == nil {
			index = backupIndex
		} else {
			// TODO There are an enormous number of these in the log.  Why?  What does it mean?
			log.Println("Couldn't get a valid Geoname id!", record)
			//TODO: Add a prometheus metric here
		}

	}
	newNode.LocationIndex = index
	newNode.PostalCode = record[6]
	newNode.Latitude, err = stringToFloat(record[7], "Latitude")
	if err != nil {
		return err
	}
	newNode.Longitude, err = stringToFloat(record[8], "Longitude")
	if err != nil {
		return err
	}
	return nil
}

// CreateNode for details see the iputils.IPNodeParser interface!
func (p *geoNodeParser) CreateNode() iputils.IPNode {
	return &GeoIPNode{}
}

// AppendNode for details see the iputils.IPNodeParser interface!
func (p *geoNodeParser) AppendNode(node iputils.IPNode) {
	n := node.(*GeoIPNode)
	p.list = append(p.list, *n)
}

// LastNode for details see the iputils.IPNodeParser interface!
func (p *geoNodeParser) LastNode() iputils.IPNode {
	if len(p.list) < 1 {
		return nil
	}
	return &p.list[len(p.list)-1]
}

func checkNumColumns(record []string, size int) error {
	if len(record) != size {
		log.Println("Incorrect number of columns in IP list", size, " got: ", len(record), record)
		return errors.New("Corrupted Data: wrong number of columns")
	}
	return nil
}

// Finds provided geonameID within idMap and returns the index in idMap
// locationIdMap := map[int]int{
//	609013: 0,
//	104084: 4,
//	17:     4,
// }
// lookupGeoID("17",locationIdMap) would return (2,nil).
// TODO: Add error metrics
func lookupGeoID(gnid string, idMap map[int]int) (int, error) {
	geonameID, err := strconv.Atoi(gnid)
	if err != nil {
		return 0, errors.New("Corrupted Data: geonameID should be a number")
	}
	loadIndex, ok := idMap[geonameID]
	if !ok {
		log.Println("geonameID not found ", geonameID)
		return 0, errors.New("Corrupted Data: geonameId not found")
	}
	return loadIndex, nil
}

func stringToFloat(str, field string) (float64, error) {
	flt, err := strconv.ParseFloat(str, 64)
	if err != nil {
		if len(str) > 0 {
			log.Println(field, " was not a number")
			output := strings.Join([]string{"Corrupted Data: ", field, " should be an int"}, "")
			return 0, errors.New(output)
		}
	}
	return flt, nil
}

// LoadIPListG2 creates a List of IPNodes from a GeoLite2 reader.
// TODO(gfr) Update to use recursion instead of stack.
// TODO(yachang) If a database fails to load, the cache should mark it as unloadable,
// the error message should indicate that we need a different dataset for that date range.
func LoadIPListG2(reader io.Reader, idMap map[int]int) ([]GeoIPNode, error) {
	parser := newGeoNodeParser(idMap)
	err := iputils.BuildIPNodeList(reader, parser)
	return parser.list, err
}
