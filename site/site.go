// Package site provides site annotations.
package site

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/m-lab/go/content"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
	uuid "github.com/m-lab/uuid-annotator/annotator"
)

var (
	// For example of how siteinfo is loaded on production servers, see
	// https://github.com/m-lab/k8s-support/blob/ff5b53faef7828d11d45c2a4f27d53077ddd080c/k8s/daemonsets/templates.jsonnet#L350
	siteinfo        = flagx.URL{}
	siteinfoRetired = flagx.URL{}
	globalAnnotator *annotator
)

func init() {
	flag.Var(&siteinfo, "siteinfo.url", "The URL for the Siteinfo JSON file containing server location and ASN metadata. gs:// and file:// schemes accepted.")
	flag.Var(&siteinfoRetired, "siteinfo.retired-url", "The URL for the Siteinfo retired JSON file. gs:// and file:// schemes accepted.")
	globalAnnotator = nil
}

// Annotate adds site annotation for a site/machine
func Annotate(ip string, server *uuid.ServerAnnotations) {
	if globalAnnotator != nil {
		globalAnnotator.Annotate(ip, server)
	}
}

// LoadFrom loads the site annotation source from the provider.
func LoadFrom(ctx context.Context, js content.Provider, retiredJS content.Provider) error {
	globalAnnotator = &annotator{
		siteinfoSource:        js,
		siteinfoRetiredSource: retiredJS,
		networks:              make(map[string]uuid.ServerAnnotations, 400),
	}
	err := globalAnnotator.load(ctx)
	log.Println(len(globalAnnotator.sites), "sites loaded")
	return err
}

// MustLoad loads the site annotations source and will call log.Fatal if the
// loading fails.
func MustLoad(timeout time.Duration) {
	err := Load(timeout)
	rtx.Must(err, "Could not load annotation db")
}

// Load loads the site annotations source. Will try at least once, retry up to
// timeout and return an error if unsuccessful.
func Load(timeout time.Duration) error {
	js, err := content.FromURL(context.Background(), siteinfo.URL)
	rtx.Must(err, "Invalid server annotations URL", siteinfo.URL.String())

	retiredJS, err := content.FromURL(context.Background(), siteinfoRetired.URL)
	rtx.Must(err, "Invalid retired server annotations URL", siteinfoRetired.URL.String())

	// When annotations are read via HTTP, which is the default, a timeout of
	// 1 minute is used for the GET request.
	// The timeout specified here must be > 1 * time.Minute for the retry loop
	// to make sense.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for ; ctx.Err() == nil; time.Sleep(time.Second) {
		err = LoadFrom(context.Background(), js, retiredJS)
		if err == nil {
			break
		}
	}
	return err
}

// annotator stores the annotations, and provides Annotate method.
type annotator struct {
	siteinfoSource        content.Provider
	siteinfoRetiredSource content.Provider
	// Each site has a single ServerAnnotations struct, which
	// is later customized for each machine.
	sites    map[string]uuid.ServerAnnotations
	networks map[string]uuid.ServerAnnotations
}

// missing is used if annotation is requested for a non-existant server.
var missing = uuid.ServerAnnotations{
	Geo: &uuid.Geolocation{
		Missing: true,
	},
	Network: &uuid.Network{
		Missing: true,
	},
}

// Annotate annotates the server with the appropriate annotations.
func (sa *annotator) Annotate(ip string, server *uuid.ServerAnnotations) {
	if server == nil {
		return
	}

	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return
	}

	// Find CIDR corresponding to the provided ip.
	// All of our subnets are /26 if IPv4, /64 if IPv6.
	var cidr string
	if parsedIP.To4() == nil {
		mask := net.CIDRMask(64, 128)
		cidr = fmt.Sprintf("%s/64", parsedIP.Mask(mask))
	} else {
		mask := net.CIDRMask(26, 32)
		cidr = fmt.Sprintf("%s/26", parsedIP.Mask(mask))
	}

	if ann, ok := sa.networks[cidr]; ok {
		ann.Network.CIDR = cidr
		*server = ann
	} else {
		*server = missing
	}
}

// load loads siteinfo dataset and returns them.
func (sa *annotator) load(ctx context.Context) error {
	// siteinfoAnnotation struct is used for parsing the json annotation source.
	type siteinfoAnnotation struct {
		Site    string
		Network struct {
			IPv4 string
			IPv6 string
		}
		Annotation uuid.ServerAnnotations
	}

	js, err := sa.siteinfoSource.Get(ctx)
	if err != nil {
		return err
	}
	var s []siteinfoAnnotation
	err = json.Unmarshal(js, &s)
	if err != nil {
		return err
	}
	// Read the retired sites JSON file, and merge it with the current sites.
	retiredJS, err := sa.siteinfoRetiredSource.Get(ctx)
	if err != nil {
		return err
	}
	var retired []siteinfoAnnotation
	err = json.Unmarshal(retiredJS, &retired)
	if err != nil {
		return err
	}
	s = append(s, retired...)
	for _, ann := range s {
		// Machine should always be empty, filled in later.
		ann.Annotation.Machine = ""

		// Make a map of CIDR -> Annotation.
		// Verify that the CIDRs are valid by trying to parse them.
		// If either the IPv4 or IPv6 CIDRs are wrong, the entry is
		// discarded. The IPv6 CIDR can be empty in some cases.
		if ann.Network.IPv4 == "" {
			continue
		}
		_, _, err := net.ParseCIDR(ann.Network.IPv4)
		if err != nil {
			log.Printf("Found incorrect IPv4 in siteinfo: %s\n",
				ann.Network.IPv4)
			continue
		}

		// Check the IPv6 CIDR only if not empty.
		if ann.Network.IPv6 != "" {
			_, _, err = net.ParseCIDR(ann.Network.IPv6)
			if err != nil {
				log.Printf("Found incorrect IPv6 in siteinfo: %s\n",
					ann.Network.IPv6)
				continue
			}
			sa.networks[ann.Network.IPv6] = ann.Annotation
		}

		sa.networks[ann.Network.IPv4] = ann.Annotation
	}

	return nil
}
