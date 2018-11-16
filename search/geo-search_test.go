package search_test

import (
	"encoding/binary"
	"errors"
	"log"
	"math/rand"
	"net"
	"testing"

	"google.golang.org/appengine/aetest"

	"github.com/m-lab/annotation-service/loader"
	"github.com/m-lab/annotation-service/parser"
	"github.com/m-lab/annotation-service/search"
)

var (
	preloadComplete       = false
	preloadStatus   error = nil
	// Preloaded by preload()
	gl2ipv4 []parser.IPNode
	gl2ipv6 []parser.IPNode
)

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
	for i < len(gl2ipv6) {
		ipMiddle := findMiddle(gl2ipv6[i].IPAddressLow, gl2ipv6[i].IPAddressHigh)
		ipBin, errBin := search.SearchBinary(gl2ipv6, ipMiddle.String())
		// Linear search, starting at current node, since it can't be earlier.
		ipLin, errLin := search.SearchList(gl2ipv6[i:], ipMiddle.String())
		if errBin != nil && errLin != nil && errBin.Error() != errLin.Error() {
			log.Println(errBin.Error(), "vs", errLin.Error())
			t.Errorf("Failed Error")
		}
		if parser.IsEqualIPNodes(ipBin, ipLin) != nil {
			log.Println("bad ", ipBin, ipLin)
			t.Errorf("Failed Binary vs Linear")
		}
		i += 100
	}

	// Test IPv4
	i = 0
	for i < len(gl2ipv4) {
		ipMiddle := findMiddle(gl2ipv4[i].IPAddressLow, gl2ipv4[i].IPAddressHigh)
		ipBin, errBin := search.SearchBinary(gl2ipv4, ipMiddle.String())
		// Linear search, starting at current node, since it can't be earlier.
		ipLin, errLin := search.SearchList(gl2ipv4[i:], ipMiddle.String())
		if errBin != nil && errLin != nil && errBin.Error() != errLin.Error() {
			log.Println(errBin.Error(), "vs", errLin.Error())
			t.Errorf("Failed Error")
		}
		if parser.IsEqualIPNodes(ipBin, ipLin) != nil {
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
			mid = parser.PlusOne(mid)
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

	for n := 0; n < b.N; n++ {
		i := rand.Intn(len(gl2ipv4))
		ipMiddle := findMiddle(gl2ipv4[i].IPAddressLow, gl2ipv4[i].IPAddressHigh)
		_, _ = search.SearchBinary(gl2ipv4, ipMiddle.String())
	}
}

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

	gl2locationList, idMap, err := parser.LoadLocListGLite2(rc)
	if err != nil {
		log.Println("Failed to LoadLocationList")
		preloadStatus = err
		return preloadStatus
	}
	if gl2locationList == nil || idMap == nil {
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

	gl2ipv4, err = parser.LoadIPListGLite2(rcIPv4, idMap)
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
	gl2ipv6, err = parser.LoadIPListGLite2(rcIPv6, idMap)
	if err != nil {
		preloadStatus = err
		return preloadStatus
	}
	preloadComplete = true
	preloadStatus = nil
	return preloadStatus
}
