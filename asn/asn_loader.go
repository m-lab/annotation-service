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

// ASNDataset holds the database in the memory
type ASNDataset struct {
	IPList []ASNIPNode
	Start  time.Time // Date from which to start using this dataset
}

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

// DataEquals checks if the ASNIPNode struct's other data than IP range equals to an other node.
func (n *ASNIPNode) DataEquals(other iputils.IPNode) bool {
	otherNode := other.(*ASNIPNode)
	return n.ASNString == otherNode.ASNString
}

//-----------------------------------------------------------------
// CUSTOM ASN PARSER IMPLEMENTATION
//-----------------------------------------------------------------

// asnNodeParser the parser object
type asnNodeParser struct {
	list []ASNIPNode
}

func createAsnNodeParser() *asnNodeParser {
	return &asnNodeParser{
		list: []ASNIPNode{},
	}
}

// PreconfigureReader for details see the iputils.IPNodeParser interface!
func (p *asnNodeParser) PreconfigureReader(reader *csv.Reader) error {
	reader.Comma = '\t'
	reader.FieldsPerRecord = expectedColumnCount
	return nil
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

// NewNode for details see the iputils.IPNodeParser interface!
func (p *asnNodeParser) CreateNode() iputils.IPNode {
	return &ASNIPNode{}
}

func (p *asnNodeParser) NodeListLen() int {
	return len(p.list)
}

func (p *asnNodeParser) AppendNode(node iputils.IPNode) {
	n := node.(*ASNIPNode)
	p.list = append(p.list, *n)
}

func (p *asnNodeParser) GetNode(idx int) iputils.IPNode {
	return &p.list[idx]
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
func loadData(fileName, datasetName string) ([]ASNIPNode, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	parser := createAsnNodeParser()
	return parser.list, iputils.BuildIPNodeList(file, parser)
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
		0,
		0,
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
