package parser

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"net"
	"regexp"
	"strconv"

	"github.com/m-lab/annotation-service/loader"
)

const (
	ipNumColumnsGlite2        = 10
	locationNumColumnsGlite2  = 13
	gLite2Prefix              = "GeoLite2-City"
	geoLite2BlocksFilenameIP4 = "GeoLite2-City-Blocks-IPv4.csv"  // Filename of ipv4 blocks file
	geoLite2BlocksFilenameIP6 = "GeoLite2-City-Blocks-IPv6.csv"  // Filename of ipv6 blocks file
	geoLite2LocationsFilename = "GeoLite2-City-Locations-en.csv" // Filename of locations file
)

func LoadGeoLite2(zip *zip.Reader) (*GeoDataset, error) {
	locations, err := loader.FindFile(geoLite2LocationsFilename, zip)
	if err != nil {
		return nil, err
	}
	// geoidMap is just a temporary map that will be discarded once the blocks are parsed
	locationNode, geoidMap, err := LoadLocListGLite2(locations)
	if err != nil {
		return nil, err
	}
	locations.Close()
	blocks4, err := loader.FindFile(geoLite2BlocksFilenameIP4, zip)

	if err != nil {
		return nil, err
	}
	ipNodes4, err := LoadIPListGLite2(blocks4, geoidMap)
	if err != nil {
		return nil, err
	}
	blocks4.Close()
	blocks6, err := loader.FindFile(geoLite2BlocksFilenameIP6, zip)

	if err != nil {
		return nil, err
	}
	ipNodes6, err := LoadIPListGLite2(blocks6, geoidMap)
	if err != nil {
		return nil, err
	}
	blocks6.Close()
	return &GeoDataset{IP4Nodes: ipNodes4, IP6Nodes: ipNodes6, LocationNodes: locationNode}, nil
}

// Finds the smallest and largest net.IP from a CIDR range
// Example: "1.0.0.0/24" -> 1.0.0.0 , 1.0.0.255
func rangeCIDR(cidr string) (net.IP, net.IP, error) {
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, nil, errors.New("Invalid CIDR IP range")
	}
	lowIp := make(net.IP, len(ip))
	copy(lowIp, ip)
	mask := ipnet.Mask
	for x, _ := range ip {
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
	return lowIp, ip, nil
}

// Create Location list for GLite2 databases
// TODO This code is a bit fragile.  Should probably parse the header and
// use that to guide the parsing of the rows.
func LoadLocListGLite2(reader io.Reader) ([]LocationNode, map[int]int, error) {
	idMap := make(map[int]int, mapMax)
	//idMap := make(map[int]int)
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
	// This will print a log every time it loads a newer location file.
	if len(first) != locationNumColumnsGlite2 && len(first) != 14 {
		log.Println("Incorrect number of columns in header, got: ", len(first), " wanted: ", locationNumColumnsGlite2)
		log.Println(first)
		if len(first) < locationNumColumnsGlite2 {
			return nil, nil, errors.New("Corrupted Data: wrong number of columns")
		}
	}
	// FieldsPerRecord is the expected column length
	// r.FieldsPerRecord = locationNumColumnsGlite2
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

			} else {
				log.Println(err, ": ", record)
				return nil, nil, err
			}
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
			return nil, nil, err
		}
		lNode.CountryCode, err = checkCaps(record[4], "Country code")
		if err != nil {
			return nil, nil, err
		}
		match, _ := regexp.MatchString(`^[^0-9]*$`, record[5])
		if match {
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
				return nil, nil, errors.New("Corrupted Data: metrocode should be a number")
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
			continue
		}
		lowIp, highIp, err := rangeCIDR(record[0])
		if err != nil {
			continue
		}
		newNode.IPAddressLow = lowIp
		newNode.IPAddressHigh = highIp
		// Look for GeoId within idMap and return index
		index, err := lookupGeoId(record[1], idMap)
		if err != nil {
			if backupIndex, err := lookupGeoId(record[2], idMap); err == nil {
				index = backupIndex
			} else {
				log.Println(err)
				//log.Println("Couldn't get a valid Geoname id!", record)
				//TODO: Add a prometheus metric here
				continue
			}

		}
		newNode.LocationIndex = index
		newNode.PostalCode = record[6]
		newNode.Latitude, err = stringToFloat(record[7], "Latitude")
		if err != nil {
			continue
		}
		newNode.Longitude, err = stringToFloat(record[8], "Longitude")
		if err != nil {
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
