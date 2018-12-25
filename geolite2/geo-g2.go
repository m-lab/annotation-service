package geolite2

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"net"
	"regexp"
	"strconv"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/loader"
)

const (
	ipNumColumnsGlite2        = 10
	glite2LocationMinColumns  = 13
	gLite2Prefix              = "GeoLite2-City"
	geoLite2BlocksFilenameIP4 = "GeoLite2-City-Blocks-IPv4.csv"  // Filename of ipv4 blocks file
	geoLite2BlocksFilenameIP6 = "GeoLite2-City-Blocks-IPv6.csv"  // Filename of ipv6 blocks file
	geoLite2LocationsFilename = "GeoLite2-City-Locations-en.csv" // Filename of locations file
)

var (
	// This is the regex used to filter for which files we want to consider acceptable for using with Geolite2
	geoLite2Regex = regexp.MustCompile(`Maxmind/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z-GeoLite2-City-CSV\.zip`)
	countryRE     = regexp.MustCompile(`^[^0-9]*$`)
)

func loadGeoLite2(zip *zip.Reader) (*GeoDataset, error) {
	locations, err := loader.FindFile(geoLite2LocationsFilename, zip)
	if err != nil {
		return nil, err
	}
	// geoidMap is just a temporary map that will be discarded once the blocks are parsed
	locationNode, geoidMap, err := LoadLocListGLite2(locations)
	locations.Close()
	if err != nil {
		return nil, err
	}

	blocks4, err := loader.FindFile(geoLite2BlocksFilenameIP4, zip)
	if err != nil {
		return nil, err
	}
	ipNodes4, err := LoadIPListGLite2(blocks4, geoidMap)
	blocks4.Close()
	if err != nil {
		return nil, err
	}
	blocks6, err := loader.FindFile(geoLite2BlocksFilenameIP6, zip)
	if err != nil {
		return nil, err
	}
	ipNodes6, err := LoadIPListGLite2(blocks6, geoidMap)
	blocks6.Close()
	if err != nil {
		return nil, err
	}
	return &GeoDataset{IP4Nodes: ipNodes4, IP6Nodes: ipNodes6, LocationNodes: locationNode}, nil
}

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

// LoadLocListGLite2 creates the Location list for GLite2 databases
// TODO This code is a bit fragile.  Should probably parse the header and
// use that to guide the parsing of the rows.
// TODO(yachang) If a database fails to load, the cache should mark it as unloadable,
// the error message should indicate that we need a different dataset for that date range.
func LoadLocListGLite2(reader io.Reader) ([]LocationNode, map[int]int, error) {
	idMap := make(map[int]int, mapMax)
	list := []LocationNode{}
	r := csv.NewReader(reader)
	// Skip the first line
	// TODO - we should parse the first line, instead of skipping it!!
	// This should set r.FieldsPerRecord.
	first, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, nil, errors.New("Empty input data")
	}
	// TODO - this is a bit hacky.  May want to improve it.
	// Older geoLite2 have 13 columns, but since 2018/03, they have 14 columns.
	// Added last column is is_in_european_union
	if len(first) != glite2LocationMinColumns {
		if len(first) < glite2LocationMinColumns {
			return nil, nil, errors.New("Corrupted Data: wrong number of columns")
		}
	}
	// FieldsPerRecord is the expected column length
	// r.FieldsPerRecord = glite2LocationMinColumns
	errorCount := 0
	maxErrorCount := 50
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			if len(record) != r.FieldsPerRecord {
				log.Println("Incorrect number of columns in IP list got: ", len(record), " wanted: ", r.FieldsPerRecord)
				log.Println(record)
				return nil, nil, errors.New("Corrupted Data: wrong number of columns")

			}
			log.Println(err, ": ", record)
			return nil, nil, err
		}
		var lNode LocationNode
		lNode.GeonameID, err = strconv.Atoi(record[0])
		if err != nil {
			if len(record[0]) > 0 {
				log.Println("GeonameID should be a number ", record[0])
				return nil, nil, errors.New("Corrupted Data: GeonameID should be a number")
			}
		}
		lNode.ContinentCode, err = checkCaps(record[2], "Continent code")
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, nil, errors.New("Too many errors during loading the dataset location list")
			}
			continue
		}
		lNode.CountryCode, err = checkCaps(record[4], "Country code")
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, nil, errors.New("Too many errors during loading the dataset location list")
			}
			continue
		}
		if countryRE.MatchString(record[5]) {
			lNode.CountryName = record[5]
		} else {
			log.Println("Country name should be letters only : ", record[5])
			return nil, nil, errors.New("Corrupted Data: country name should be letters")
		}
		// TODO - should probably do some validation.
		lNode.RegionCode = record[6]
		lNode.RegionName = record[7]
		lNode.MetroCode, err = strconv.ParseInt(record[11], 10, 64)
		if err != nil {
			if len(record[11]) > 0 {
				log.Println("MetroCode should be a number")
				errorCount++
				if errorCount > maxErrorCount {
					return nil, nil, errors.New("Too many errors during loading the dataset location list")
				}
				continue
			}
		}
		lNode.CityName = record[10]
		list = append(list, lNode)
		idMap[lNode.GeonameID] = len(list) - 1
	}
	return list, idMap, nil
}

// LoadIPListGLite2 creates a List of IPNodes from a GeoLite2 reader.
// TODO(gfr) Update to use recursion instead of stack.
// TODO(yachang) If a database fails to load, the cache should mark it as unloadable,
// the error message should indicate that we need a different dataset for that date range.
func LoadIPListGLite2(reader io.Reader, idMap map[int]int) ([]IPNode, error) {
	list := []IPNode{}
	r := csv.NewReader(reader)
	stack := []IPNode{}
	// Skip first line
	_, err := r.Read()
	if err == io.EOF {
		log.Println("Empty input data")
		return nil, errors.New("Empty input data")
	}
	errorCount := 0
	maxErrorCount := 50
	for {
		var newNode IPNode
		// Example:
		// GLite2 : record = [2a04:97c0::/29,2658434,2658434,0,0,47,8,100]
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		err = checkNumColumns(record, ipNumColumnsGlite2)
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, errors.New("Too many errors during loading the dataset IP list.")
			}
			continue
		}
		lowIP, highIP, err := rangeCIDR(record[0])
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, errors.New("Too many errors during loading the dataset IP list")
			}
			continue
		}
		newNode.IPAddressLow = lowIP
		newNode.IPAddressHigh = highIP
		// Look for GeoId within idMap and return index
		index, err := lookupGeoID(record[1], idMap)
		if err != nil {
			if backupIndex, err := lookupGeoID(record[2], idMap); err == nil {
				index = backupIndex
			} else {
				// TODO There are an enormous number of these in the log.  Why?  What does it mean?
				log.Printf("Couldn't get a valid Geoname id! %#v\n", record)
				//TODO: Add a prometheus metric here
			}
		}
		// TODO - if error above, this might default to zero!!
		newNode.LocationIndex = index
		newNode.PostalCode = record[6]
		newNode.Latitude, err = stringToFloat(record[7], "Latitude")
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, errors.New("Too many errors during loading the dataset IP list")
			}
			continue
		}
		newNode.Longitude, err = stringToFloat(record[8], "Longitude")
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > maxErrorCount {
				return nil, errors.New("Too many errors during loading the dataset IP list")
			}
			continue
		}
		stack, list = handleStack(stack, list, newNode)
	}
	var pop IPNode
	pop, stack = stack[len(stack)-1], stack[:len(stack)-1]
	for ; len(stack) > 0; pop, stack = stack[len(stack)-1], stack[:len(stack)-1] {
		peek := stack[len(stack)-1]
		peek.IPAddressLow = PlusOne(pop.IPAddressHigh)
		list = append(list, peek)
	}
	return list, nil
}

// LoadGeoLite2Dataset load the Geolite2 dataset with filename from bucket.
func LoadGeoLite2Dataset(filename string, bucketname string) (*GeoDataset, error) {
	zip, err := loader.CreateZipReader(context.Background(), bucketname, filename)
	log.Println("Loading dataset from", filename)
	if err != nil {
		return nil, err
	}
	dataset, err := loadGeoLite2(zip)
	if err != nil {
		return nil, err
	}
	date, err := api.ExtractDateFromFilename(filename)
	if err != nil {
		log.Println("Error extracting date:", filename)
	} else {
		dataset.start = date
		log.Println("Loaded", date.Format("20060102"), filename)
	}
	return dataset, nil
}
