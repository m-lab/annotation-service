package legacy

import (
	"encoding/csv"
	"io"
	"os"
)

var (
	// fips2ISOMapFile is the name of the FIPS to ISO csv file.
	fips2ISOMapFile = "data/fips-iso-map.csv"

	// fipsMap is a singleton, package pointer to a map of FIPS-10 to ISO 3166-2
	// Region codes and names.
	fips2ISOMap map[string]subdivision
)

type subdivision struct {
	ISOCode string
	Name    string
}

func fipsKey(country, region string) string {
	return country + "-" + region
}

func parseFips2ISOMap(name string) (map[string]subdivision, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(f)
	// Read & discard first row as header.
	// Header: Country ISO Code,Region FIPS Code,Region ISO Code,Region Name
	_, err = reader.Read()
	if err != nil {
		return nil, err
	}

	fmap := map[string]subdivision{}
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		key := fipsKey(row[0], row[1])
		fmap[key] = subdivision{
			ISOCode: row[2],
			Name:    row[3],
		}
	}
	return fmap, nil
}
