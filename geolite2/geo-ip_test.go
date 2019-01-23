package geolite2_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
	"github.com/m-lab/annotation-service/loader"
	"google.golang.org/appengine/aetest"
)

// This just allows compiler to check that GeoDataset satisfies the Finder interface.
func assertAnnotator(f api.Annotator) {
	func(api.Annotator) {}(&geolite2.GeoDataset{})
}

// Returns nil if two IP Lists are equal
func isEqualIPLists(listComp, list []geolite2.IPNode) error {
	for index, element := range list {
		err := geolite2.IsEqualIPNodes(element, listComp[index])
		if err != nil {
			return err
		}
	}
	return nil
}

// Returns nil if two Location lists are equal
func isEqualLocLists(list, listComp []geolite2.LocationNode) error {
	for index, element := range list {
		if index >= len(listComp) {
			output := fmt.Sprint("Out of range:", index)
			log.Println(output)
			return errors.New(output)
		}
		if element.GeonameID != listComp[index].GeonameID {
			output := strings.Join([]string{"GeonameID inconsistent\ngot:", strconv.Itoa(element.GeonameID), " \nwanted:", strconv.Itoa(listComp[index].GeonameID)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.ContinentCode != listComp[index].ContinentCode {
			output := strings.Join([]string{"Continent code inconsistent\ngot:", element.ContinentCode, " \nwanted:", listComp[index].ContinentCode}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CountryCode != listComp[index].CountryCode {
			output := strings.Join([]string{"Country code inconsistent\ngot:", element.CountryCode, " \nwanted:", listComp[index].CountryCode}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CountryName != listComp[index].CountryName {
			output := strings.Join([]string{"Country name inconsistent\ngot:", element.CountryName, " \nwanted:", listComp[index].CountryName}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.MetroCode != listComp[index].MetroCode {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", strconv.FormatInt(element.MetroCode, 16), " \nwanted:", strconv.FormatInt(listComp[index].MetroCode, 16)}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.CityName != listComp[index].CityName {
			output := strings.Join([]string{"Longitude inconsistent\ngot:", element.CityName, " \nwanted:", listComp[index].CityName}, "")
			log.Println(output)
			return errors.New(output)
		}
		if element.RegionName != listComp[index].RegionName {
			output := strings.Join([]string{"RegionName inconsistent\ngot:", element.RegionName, " \nwanted:", listComp[index].RegionName}, "")
			log.Println(output)
			return errors.New(output)
		}
	}
	return nil
}

func TestPopulateLocationData(t *testing.T) {
	tests := []struct {
		node geolite2.IPNode
		locs []geolite2.LocationNode
		res  api.GeoData
	}{
		{
			node: geolite2.IPNode{LocationIndex: 0, PostalCode: "10583"},
			locs: []geolite2.LocationNode{{CityName: "Not A Real City", RegionCode: "ME"}},
			res: api.GeoData{
				Geo: &api.GeolocationIP{City: "Not A Real City", PostalCode: "10583", Region: "ME"},
				ASN: nil},
		},
		{
			node: geolite2.IPNode{LocationIndex: -1, PostalCode: "10583"},
			locs: nil,
			res: api.GeoData{
				Geo: &api.GeolocationIP{PostalCode: "10583"},
				ASN: nil},
		},
	}
	for _, test := range tests {
		data := api.GeoData{}
		geolite2.PopulateLocationData(test.node, test.locs, &data)
		if diff := deep.Equal(data, test.res); diff != nil {
			t.Error(diff)
		}
	}
}

var (
	preloadComplete       = false
	preloadStatus   error = nil
	// Preloaded by preload()
	annotator = geolite2.GeoDataset{}
	//gl2ipv4 []geolite2.IPNode
	//gl2ipv6 []geolite2.IPNode
)

// Returns a geolite2.IPNode with the smallet range that includes the provided IP address
func searchList(list []geolite2.IPNode, ipLookUp string) (geolite2.IPNode, error) {
	inRange := false
	var lastNodeIndex int
	userIP := net.ParseIP(ipLookUp)
	if userIP == nil {
		log.Println("Inputed IP string could not be parsed to net.IP")
		return geolite2.IPNode{}, errors.New("Invalid search IP")
	}
	for i := range list {
		if bytes.Compare(userIP, list[i].IPAddressLow) >= 0 && bytes.Compare(userIP, list[i].IPAddressHigh) <= 0 {
			inRange = true
			lastNodeIndex = i
		} else if inRange && bytes.Compare(userIP, list[i].IPAddressLow) < 0 {
			return list[lastNodeIndex], nil
		}
	}
	if inRange {
		return list[lastNodeIndex], nil
	}
	return geolite2.IPNode{}, errors.New("Node not found\n")
}

func TestGeoLite2(t *testing.T) {
	err := preload()
	if err != nil {
		// TODO: make CreateZipReader produce identifiable error types
		// and then skip things when it has an auth failure but
		// t.Error() if the problem is anything other than an auth
		// failure.
		log.Println(err)
		log.Println("This statement errors out when things are being tested from github repos that are not github.com/m-lab/annotation-server.  We are assuming that this is the case, and skipping the rest of this test.")
		return
	}

	i := 0
	gl2ipv6 := annotator.IP6Nodes
	for i < len(gl2ipv6) {
		ipMiddle := findMiddle(gl2ipv6[i].IPAddressLow, gl2ipv6[i].IPAddressHigh)
		ipBin, errBin := annotator.SearchBinary(ipMiddle.String())
		// Linear search, starting at current node, since it can't be earlier.
		ipLin, errLin := searchList(gl2ipv6[i:], ipMiddle.String())
		if errBin != nil && errLin != nil && errBin.Error() != errLin.Error() {
			log.Println(errBin.Error(), "vs", errLin.Error())
			t.Errorf("Failed Error")
		}
		if geolite2.IsEqualIPNodes(ipBin, ipLin) != nil {
			log.Println("bad ", ipBin, ipLin)
			t.Errorf("Failed Binary vs Linear")
		}
		i += 100
	}

	// Test IPv4
	i = 0
	gl2ipv4 := annotator.IP4Nodes
	for i < len(gl2ipv4) {
		ipMiddle := findMiddle(gl2ipv4[i].IPAddressLow, gl2ipv4[i].IPAddressHigh)

		ipBin, errBin := annotator.SearchBinary(ipMiddle.String())
		// Linear search, starting at current node, since it can't be earlier.
		ipLin, errLin := searchList(gl2ipv4[i:], ipMiddle.String())
		if errBin != nil && errLin != nil && errBin.Error() != errLin.Error() {
			log.Println(errBin.Error(), "vs", errLin.Error())
			t.Errorf("Failed Error")
		}
		if geolite2.IsEqualIPNodes(ipBin, ipLin) != nil {
			log.Println("bad ", ipBin, ipLin)
			t.Errorf("Failed Binary vs Linear")
		}
		i += 100
	}

}

// TODO(gfr) This needs good comment and validation?
func findMiddle(low, high net.IP) net.IP {
	lowInt := binary.BigEndian.Uint32(low[12:16])
	highInt := binary.BigEndian.Uint32(high[12:16])
	middleInt := int((highInt - lowInt) / 2)
	mid := low
	i := 0
	if middleInt < 100000 {
		for i < middleInt/2 {
			mid = geolite2.PlusOne(mid)
			i++
		}
	}
	return mid
}

func BenchmarkGeoLite2ipv4(b *testing.B) {
	err := preload()
	if err != nil {
		// TODO: make CreateZipReader produce identifiable error types
		// and then skip things when it has an auth failure but
		// t.Error() if the problem is anything other than an auth
		// failure.
		log.Println(err)
		log.Println("This statement errors out when things are being tested from github repos that are not github.com/m-lab/annotation-server.  We are assuming that this is the case, and skipping the rest of this test.")
		return
	}

	b.ResetTimer()

	gl2ipv4 := annotator.IP4Nodes
	for n := 0; n < b.N; n++ {
		i := rand.Intn(len(gl2ipv4))
		ipMiddle := findMiddle(gl2ipv4[i].IPAddressLow, gl2ipv4[i].IPAddressHigh)
		_, _ = annotator.SearchBinary(ipMiddle.String())
	}
}

// TODO - can this just use the standard loader now?
func preload() error {
	if preloadComplete {
		return preloadStatus
	}
	preloadComplete = true

	ctx, done, err := aetest.NewContext()
	if err != nil {
		preloadStatus = err
		return preloadStatus
	}
	defer done()
	reader, err := loader.CreateZipReader(ctx, "test-annotator-sandbox", "MaxMind/2017/09/07/Maxmind%2F2017%2F09%2F07%2F20170907T023620Z-GeoLite2-City-CSV.zip")
	if err != nil {
		preloadStatus = err
		return preloadStatus
	}

	// Load Location list
	rc, err := loader.FindFile("GeoLite2-City-Locations-en.csv", reader)
	if err != nil {
		preloadStatus = err
		return preloadStatus
	}
	defer rc.Close()

	var idMap map[int]int
	annotator.LocationNodes, idMap, err = geolite2.LoadLocListGLite2(rc)
	if err != nil {
		log.Println("Failed to LoadLocationList")
		preloadStatus = err
		return preloadStatus
	}
	if annotator.LocationNodes == nil || idMap == nil {
		preloadStatus = errors.New("Failed to create LocationList and mapID")
		return preloadStatus
	}

	// Benchmark IPv4
	rcIPv4, err := loader.FindFile("GeoLite2-City-Blocks-IPv4.csv", reader)
	if err != nil {
		preloadStatus = err
		return preloadStatus
	}
	defer rcIPv4.Close()

	annotator.IP4Nodes, err = geolite2.LoadIPListGLite2(rcIPv4, idMap)
	if err != nil {
		preloadStatus = err
		return preloadStatus
	}

	// Test IPv6
	rcIPv6, err := loader.FindFile("GeoLite2-City-Blocks-IPv6.csv", reader)
	if err != nil {
		preloadStatus = errors.New("Failed to create io.ReaderCloser")
		return preloadStatus
	}
	defer rcIPv6.Close()
	// TODO: update tests to use high level data loader functions instead of low level funcs
	annotator.IP6Nodes, err = geolite2.LoadIPListGLite2(rcIPv6, idMap)
	if err != nil {
		preloadStatus = err
		return preloadStatus
	}

	preloadComplete = true
	preloadStatus = nil
	return preloadStatus
}
