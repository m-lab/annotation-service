package geolite2v2_test

// TODO - migrate these tests to geolite2v2 before removing geolite2 package

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

	"github.com/m-lab/annotation-service/geolite2v2"
	"github.com/m-lab/annotation-service/geoloader"
	"github.com/m-lab/annotation-service/iputils"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/geolite2"
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
		node geolite2v2.GeoIPNode
		locs []geolite2v2.LocationNode
		res  api.GeoData
	}{
		{
			node: geolite2v2.GeoIPNode{LocationIndex: 0, PostalCode: "10583"},
			locs: []geolite2v2.LocationNode{{CityName: "Not A Real City", RegionCode: "ME"}},
			res: api.GeoData{
				Geo:     &api.GeolocationIP{City: "Not A Real City", PostalCode: "10583", Region: "ME"},
				Network: nil},
		},
		{
			node: geolite2v2.GeoIPNode{LocationIndex: -1, PostalCode: "10583"},
			locs: nil,
			res: api.GeoData{
				Geo:     &api.GeolocationIP{PostalCode: "10583"},
				Network: nil},
		},
	}
	for _, test := range tests {
		data := api.GeoData{}
		geolite2v2.PopulateLocationData(&test.node, test.locs, &data)
		if diff := deep.Equal(data, test.res); diff != nil {
			t.Error(diff)
		}
	}
}

var (
	preloadComplete       = false
	preloadStatus   error = nil
	// Preloaded by preload()
	annotator *geolite2v2.GeoDataset
	//gl2ipv4 []geolite2.IPNode
	//gl2ipv6 []geolite2.IPNode
)

// Returns a geolite2.IPNode with the smallet range that includes the provided IP address
// TODO - should these be iputils.IPNode instead of GeoIPNode?
func searchList(list []geolite2v2.GeoIPNode, ipLookUp string) (iputils.IPNode, error) {
	inRange := false
	var lastNodeIndex int
	userIP := net.ParseIP(ipLookUp)
	if userIP == nil {
		log.Println("Inputed IP string could not be parsed to net.IP")
		return nil, errors.New("Invalid search IP")
	}
	for i := range list {
		if bytes.Compare(userIP, list[i].IPAddressLow) >= 0 && bytes.Compare(userIP, list[i].IPAddressHigh) <= 0 {
			inRange = true
			lastNodeIndex = i
		} else if inRange && bytes.Compare(userIP, list[i].IPAddressLow) < 0 {
			return &list[lastNodeIndex], nil
		}
	}
	if inRange {
		return &list[lastNodeIndex], nil
	}
	return nil, errors.New("Node not found\n")
}

func randomValidIPv6(ann api.Annotator) (int, net.IP) {
	switch v := ann.(type) {
	case *geolite2.GeoDataset:
		gl2ipv6 := v.IP6Nodes
		i := rand.Intn(len(gl2ipv6))
		ipMiddle := findMiddle(gl2ipv6[i].IPAddressLow, gl2ipv6[i].IPAddressHigh)
		return i, ipMiddle
	case *geolite2v2.GeoDataset:
		gl2ipv6 := v.IP6Nodes
		i := rand.Intn(len(gl2ipv6))
		ipMiddle := findMiddle(gl2ipv6[i].IPAddressLow, gl2ipv6[i].IPAddressHigh)
		return i, ipMiddle
	default:
		return 0, nil
	}
}

func randomValidIPv4(ann api.Annotator) (int, net.IP) {
	switch v := ann.(type) {
	case *geolite2.GeoDataset:
		gl2ipv4 := v.IP4Nodes
		i := rand.Intn(len(gl2ipv4))
		ipMiddle := findMiddle(gl2ipv4[i].IPAddressLow, gl2ipv4[i].IPAddressHigh)
		return i, ipMiddle
	case *geolite2v2.GeoDataset:
		gl2ipv4 := v.IP4Nodes
		i := rand.Intn(len(gl2ipv4))
		ipMiddle := findMiddle(gl2ipv4[i].IPAddressLow, gl2ipv4[i].IPAddressHigh)
		return i, ipMiddle
	default:
		return 0, nil
	}
}

func TestGeoLite2SearchBinary(t *testing.T) {
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

	v6errMatch := 0
	v6ipMatch := 0
	gl2ipv6 := annotator.IP6Nodes
	for i := 0; i < 10000; i++ {
		idx, v6 := randomValidIPv6(annotator)
		ipBin, errBin := annotator.SearchBinary(v6.String())
		// Linear search, starting at current node, since it can't be earlier.
		ipLin, errLin := searchList(gl2ipv6[idx:], v6.String())
		if errBin != nil || errLin != nil {
			if errBin != nil && errLin != nil {
				if errBin.Error() != errLin.Error() {
					log.Println(errBin.Error(), "vs", errLin.Error())
					t.Errorf("Failed Error")
				}
				v6errMatch++
			} else {
				t.Error(errBin, "!=", errLin)
			}
		} else if geolite2v2.IsEqualIPNodes(ipBin, ipLin) != nil {
			log.Println("bad ", ipBin, ipLin)
			t.Errorf("Failed Binary vs Linear")
		}
		v6ipMatch++
		i += 100
	}

	// Test IPv4
	gl2ipv4 := annotator.IP4Nodes
	v4errMatch := 0
	v4ipMatch := 0
	for i := 0; i < 10000; i++ {
		idx, v4 := randomValidIPv4(annotator)
		ipBin, errBin := annotator.SearchBinary(v4.String())
		// Linear search, starting at current node, since it can't be earlier.
		ipLin, errLin := searchList(gl2ipv4[idx:], v4.String())
		if errBin != nil || errLin != nil {
			if errBin != nil && errLin != nil {
				if errBin.Error() != errLin.Error() {
					log.Println(errBin.Error(), "vs", errLin.Error())
					t.Errorf("Failed Error")
				}
				v4errMatch++
			} else {
				t.Error(errBin, "!=", errLin)
			}
		} else if geolite2v2.IsEqualIPNodes(ipBin, ipLin) != nil {
			log.Println("bad ", ipBin, ipLin)
			t.Errorf("Failed Binary vs Linear")
		}
		v4ipMatch++
		i += 100
	}

	t.Logf("Found %d matching err and %d matching ip for v4", v4errMatch, v4ipMatch)
	t.Logf("Found %d matching err and %d matching ip for v6", v6errMatch, v6ipMatch)
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
	// TODO - for some reason, we are still seeing March 2018 instead of Sept 2017.
	year, month, day := "2017", "09", "07"
	geoloader.UseSpecificGeolite2DateForTesting(&year, &month, &day)
	g2loader := geoloader.Geolite2Loader(geolite2v2.LoadG2)
	err := g2loader.UpdateCache()
	if err != nil {
		preloadStatus = err
		return preloadStatus
	}

	annotator = g2loader.Fetch()[0].(*geolite2v2.GeoDataset)
	preloadComplete = true
	preloadStatus = nil
	return preloadStatus
}
