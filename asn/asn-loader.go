package asn

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/iputils"
	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/uuid-annotator/ipinfo"
)

var (
	expectedColumnCount = 3 // the number of the expected columns in the source dataset

	errExtractDateFromFilename = errors.New("cannot extract date from input filename")

	// ASNamesFile names the ASN source data.
	ASNamesFile = "data/asnames.ipinfo.csv"

	// asnames contains the AS number -> AS name association, loaded from
	// ASNamesFile. Each annotator keeps a reference to this global map, so
	// that we don't need to load the file multiple times.
	asnames ipinfo.ASNames

	// once is used to make sure loading ASNamesFile only happens once.
	once sync.Once
)

// ASNDataset holds the database in the memory
type ASNDataset struct {
	IPList  []ASNIPNode
	ASNames ipinfo.ASNames
	Start   time.Time // Date from which to start using this dataset
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

func (p *asnNodeParser) AppendNode(node iputils.IPNode) {
	n := node.(*ASNIPNode)
	p.list = append(p.list, *n)
}

// LastNode for details see the iputils.IPNodeParser interface!
func (p *asnNodeParser) LastNode() iputils.IPNode {
	if len(p.list) < 1 {
		return nil
	}
	return &p.list[len(p.list)-1]
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

	once.Do(func() {
		// Load the ipinfo CSV containing the ASN -> ASName mapping.
		content, err := ioutil.ReadFile(ASNamesFile)
		rtx.Must(err, "Cannot load asnames files")
		asnames, err = ipinfo.Parse(content)
		rtx.Must(err, "Cannot parse asnames file")
	})

	return &ASNDataset{IPList: nodes, Start: *time, ASNames: asnames}, nil
}

// LoadASNDatasetFromReader produces a new ASN api.Annotator.
func LoadASNDatasetFromReader(file io.Reader) (api.Annotator, error) {
	parser := createAsnNodeParser()
	err := iputils.BuildIPNodeList(file, parser)
	if err != nil {
		return nil, err
	}
	return &ASNDataset{IPList: parser.list}, nil
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
	if groups == nil || len(groups) < 4 {
		log.Printf("Could not extract time from ASN filename: %s\n", fileName)
		return nil, errExtractDateFromFilename
	}

	yearInt, erry := strconv.Atoi(groups[1])
	monthInt, errm := strconv.Atoi(groups[2])
	dayInt, errd := strconv.Atoi(groups[3])

	if erry != nil || errm != nil || errd != nil {
		return nil, errExtractDateFromFilename
	}

	// Based on the docs, the time should be ment in UTC
	t := time.Date(
		yearInt,
		time.Month(monthInt),
		dayInt,
		0,
		0,
		0,
		0,
		time.UTC)

	return &t, nil
}
