package asn

import (
	"context"
	"encoding/csv"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/iputils"
	"github.com/m-lab/annotation-service/loader"
)

var (
	expectedColumnCount = 3 // the number of the expected columns in the source dataset
)

//-----------------------------------------------------------------
// CUSTOM ASN IPNode DEFINITION
//-----------------------------------------------------------------

// ASNIPNode represents a node in the cached list
type ASNIPNode struct {
	iputils.BaseIPNode
	ASNString string
}

// Clone clones the ASNIPNode struct to satistfy the IPNode interface
func (n *ASNIPNode) Clone() iputils.IPNode {
	return &ASNIPNode{BaseIPNode: iputils.BaseIPNode{IPAddressLow: n.IPAddressLow, IPAddressHigh: n.IPAddressHigh}, ASNString: n.ASNString}
}

//-----------------------------------------------------------------
// CUSTOM ASN PARSER IMPLEMENTATION
//-----------------------------------------------------------------

// asnNodeParser the parser object
type asnNodeParser struct{}

// PreconfigureReader for details see the iputils.IPNodeParser interface!
func (p *asnNodeParser) PreconfigureReader(reader *csv.Reader) error {
	reader.Comma = '\t'
	reader.FieldsPerRecord = expectedColumnCount
	return nil
}

// NewNode for details see the iputils.IPNodeParser interface!
func (p *asnNodeParser) NewNode() iputils.IPNode {
	return &ASNIPNode{}
}

// ValidateRecord for details see the iputils.IPNodeParser interface!
func (p *asnNodeParser) ValidateRecord(record []string) error {
	return nil
}

// ExtractIP for details see the iputils.IPNodeParser interface!
func (p *asnNodeParser) ExtractIP(record []string) string {
	return strings.Join(record[:2], "/")
}

// PopulateRecordData for details see the iputils.IPNodeParser interface!
func (p *asnNodeParser) PopulateRecordData(record []string, node iputils.IPNode) error {
	asnNode, ok := node.(*ASNIPNode)
	if !ok {
		return ErrorIllegalIPNodeType
	}
	asnNode.ASNString = record[2]
	return nil
}

//-----------------------------------------------------------------
// DATASET LOADER IMPLEMENTATION
//-----------------------------------------------------------------

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

// loadData loads the data into an ASNIPNode list
// FIXME eliminate, it's a copy-paste of LoadIPListGLite2 from geo-g2.go
func loadData(fileName, datasetName string) ([]iputils.IPNode, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	parser := &asnNodeParser{}
	return iputils.BuildIPNodeList(file, parser)
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
