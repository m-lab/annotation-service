package geolite2v2

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/m-lab/annotation-service/loader"
)

var (
	glite2LocationMinColumns = 13
	capsRE                   = regexp.MustCompile("^[0-9A-Z]*$")
	countryRE                = regexp.MustCompile(`^[^0-9]*$`)
)

// LocationNode defines Location databases
type LocationNode struct {
	GeonameID     int
	ContinentCode string
	CountryCode   string
	CountryName   string
	RegionCode    string
	RegionName    string
	MetroCode     int64
	CityName      string
}

type locationCsvConsumer struct {
	locationMap     map[int]int
	locationList    []LocationNode
	fieldsPerRecord int
}

func newLocationCsvConsumer() *locationCsvConsumer {
	l := locationCsvConsumer{
		locationMap:  make(map[int]int, mapMax),
		locationList: []LocationNode{},
	}
	return &l
}

func (l *locationCsvConsumer) PreconfigureReader(reader *csv.Reader) error {
	// Skip the first line
	// TODO - we should parse the first line, instead of skipping it!!
	// This should set r.FieldsPerRecord.
	first, err := reader.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return errors.New("Empty input data")
	}
	// TODO - this is a bit hacky.  May want to improve it.
	// Older geoLite2 have 13 columns, but since 2018/03, they have 14 columns.
	// Added last column is is_in_european_union
	if len(first) != glite2LocationMinColumns {
		if len(first) < glite2LocationMinColumns {
			return errors.New("Corrupted Data: wrong number of columns")
		}
	}
	l.fieldsPerRecord = reader.FieldsPerRecord
	return nil
}

func (l *locationCsvConsumer) ValidateRecord(record []string) error {
	if len(record) != l.fieldsPerRecord {
		log.Println("Incorrect number of columns in IP list got: ", len(record), " wanted: ", l.fieldsPerRecord)
		log.Println(record)
		return errors.New("Corrupted Data: wrong number of columns")
	}
	return nil
}

func (l *locationCsvConsumer) Consume(record []string) error {
	var lNode LocationNode
	var err error
	lNode.GeonameID, err = strconv.Atoi(record[0])
	if err != nil {
		if len(record[0]) > 0 {
			log.Println("GeonameID should be a number ", record[0])
			return errors.New("Corrupted Data: GeonameID should be a number")
		}
	}
	lNode.ContinentCode, err = checkCaps(record[2], "Continent code")
	if err != nil {
		return err
	}
	lNode.CountryCode, err = checkCaps(record[4], "Country code")
	if err != nil {
		return err
	}
	if countryRE.MatchString(record[5]) {
		lNode.CountryName = record[5]
	} else {
		log.Println("Country name should be letters only : ", record[5])
		return errors.New("Corrupted Data: country name should be letters")
	}
	// TODO - should probably do some validation.
	lNode.RegionCode = record[6]
	lNode.RegionName = record[7]
	lNode.MetroCode, err = strconv.ParseInt(record[11], 10, 64)
	if err != nil {
		if len(record[11]) > 0 {
			log.Println("MetroCode should be a number")
			return err
		}
	}
	lNode.CityName = record[10]
	l.locationList = append(l.locationList, lNode)
	l.locationMap[lNode.GeonameID] = len(l.locationList) - 1
	return nil
}

// checkCaps ensures that field name contains only upper case A-Z and digits 0-9.
func checkCaps(str, field string) (string, error) {
	if capsRE.MatchString(str) {
		return str, nil
	}
	log.Println(field, "should be all capitals and no punctuation: ", str)
	output := strings.Join([]string{"Corrupted Data: ", field, " should be all caps and no punctuation"}, "")
	return "", errors.New(output)
}

// LoadLocationsG2 creates the Location list for GLite2 databases
// TODO This code is a bit fragile.  Should probably parse the header and
// use that to guide the parsing of the rows.
// TODO(yachang) If a database fails to load, the cache should mark it as unloadable,
// the error message should indicate that we need a different dataset for that date range.
func LoadLocationsG2(reader io.Reader) ([]LocationNode, map[int]int, error) {
	consumer := newLocationCsvConsumer()
	r := loader.NewCSVReader(reader, consumer)
	err := r.ReadAll()
	if err != nil {
		return nil, nil, err
	}
	return consumer.locationList, consumer.locationMap, nil
}