package legacy

import (
	"encoding/csv"
	"io"
	"os"
)

var (
	// Fips2ISOMapFile is the name of the FIPS to ISO csv file.
	// Download: https://dev.maxmind.com/wp-content/uploads/2020/06/fips-iso-map.csv
	// Plus added supplemental region names for US and CA.
	Fips2ISOMapFile = "data/fips-iso-map.csv"

	// fipsMap is a singleton, package variable that maps FIPS-10 to ISO 3166-2
	// Region codes and names.
	fips2ISOMap map[string]subdivision
)

// subdivision contains the ISO 3166-2 subdivision1 iso code and name.
type subdivision struct {
	ISOCode string
	Name    string
}

func fipsKey(country, region string) string {
	return country + "-" + region
}

// parseFips2ISOMap reads the CSV content of the filename, parses it and returns
// a map of the (country,FIPS region) mapped to the ISO (code,name). The map key
// is generated using `fipsKey()`.
func parseFips2ISOMap(filename string) (map[string]subdivision, error) {
	f, err := os.Open(filename)
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
