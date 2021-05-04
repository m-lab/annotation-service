package site_test

import (
	"context"
	"flag"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/site"
	"github.com/m-lab/go/content"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/osx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/uuid-annotator/annotator"
)

type badProvider struct {
	err error
}

func (b badProvider) Get(_ context.Context) ([]byte, error) {
	return nil, b.err
}

var (
	localRawfile content.Provider
	corruptFile  content.Provider
	retiredFile  content.Provider
)

func setUp() {
	u, err := url.Parse("file:testdata/annotations.json")
	rtx.Must(err, "Could not parse URL")
	localRawfile, err = content.FromURL(context.Background(), u)
	rtx.Must(err, "Could not create content.Provider")

	u, err = url.Parse("file:testdata/corrupt-annotations.json")
	rtx.Must(err, "Could not parse URL")
	corruptFile, err = content.FromURL(context.Background(), u)
	rtx.Must(err, "Could not create content.Provider")

	u, err = url.Parse("file:testdata/retired-annotations.json")
	rtx.Must(err, "Could not parse URL")
	retiredFile, err = content.FromURL(context.Background(), u)
	rtx.Must(err, "Could not create content.Provider")
}

func TestBasic(t *testing.T) {
	setUp()
	ctx := context.Background()
	site.LoadFrom(ctx, localRawfile, retiredFile)

	var missingServerAnn = annotator.ServerAnnotations{
		Geo: &annotator.Geolocation{
			Missing: true,
		},
		Network: &annotator.Network{
			Missing: true,
		},
	}

	defaultServerAnnIPv4 := annotator.ServerAnnotations{
		Machine: "",
		Site:    "lga03",
		Geo: &annotator.Geolocation{
			ContinentCode: "NA",
			CountryCode:   "US",
			City:          "New York",
			Latitude:      40.7667,
			Longitude:     -73.8667,
		},
		Network: &annotator.Network{
			CIDR:     "64.86.148.128/26",
			ASNumber: 6453,
			ASName:   "TATA COMMUNICATIONS (AMERICA) INC",
			Systems: []annotator.System{
				{ASNs: []uint32{6453}},
			},
		},
	}

	defaultServerAnnIPv6 := annotator.ServerAnnotations{
		Machine: "",
		Site:    "lga03",
		Geo: &annotator.Geolocation{
			ContinentCode: "NA",
			CountryCode:   "US",
			City:          "New York",
			Latitude:      40.7667,
			Longitude:     -73.8667,
		},
		Network: &annotator.Network{
			CIDR:     "2001:5a0:4300::/64",
			ASNumber: 6453,
			ASName:   "TATA COMMUNICATIONS (AMERICA) INC",
			Systems: []annotator.System{
				{ASNs: []uint32{6453}},
			},
		},
	}

	retiredServerann := annotator.ServerAnnotations{
		Machine: "",
		Site:    "acc01",
		Geo: &annotator.Geolocation{
			ContinentCode: "AF",
			CountryCode:   "GH",
			City:          "Accra",
			Latitude:      5.606,
			Longitude:     -0.1681,
		},
		Network: &annotator.Network{
			CIDR:     "196.201.2.192/26",
			ASNumber: 30997,
			ASName:   "Ghana Internet Exchange Association",
			Systems: []annotator.System{
				{ASNs: []uint32{30997}},
			},
		},
	}

	tests := []struct {
		name string
		ip   string
		want annotator.ServerAnnotations
	}{
		{
			name: "success",
			ip:   "64.86.148.130",
			want: defaultServerAnnIPv4,
		},
		{
			name: "success-ipv6",
			ip:   "2001:5a0:4300::1",
			want: defaultServerAnnIPv6,
		},
		{
			name: "success-retired-site",
			ip:   "196.201.2.192",
			want: retiredServerann,
		},
		{
			name: "missing",
			ip:   "0.0.0.0",
			want: missingServerAnn,
		},
		{
			name: "missing-ipv6",
			ip:   "::1",
			want: missingServerAnn,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ann := annotator.ServerAnnotations{}
			site.Annotate(tt.ip, &ann)
			if diff := deep.Equal(ann, tt.want); diff != nil {
				t.Errorf("Annotate() failed; %s", strings.Join(diff, "\n"))
			}
		})
	}
}

func TestMustLoad(t *testing.T) {
	cleanupURL := osx.MustSetenv("SITEINFO_URL", "file:testdata/annotations.json")
	defer cleanupURL()
	cleanupRetiredURL := osx.MustSetenv("SITEINFO_RETIRED_URL", "file:testdata/retired-annotations.json")
	defer cleanupRetiredURL()
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from environment variables")

	site.MustLoad(5 * time.Second)
}

func TestNilServer(t *testing.T) {
	setUp()
	ctx := context.Background()
	err := site.LoadFrom(ctx, localRawfile, retiredFile)
	if err != nil {
		t.Error(err)
	}
	// Should not panic!  Nothing else to check.
	site.Annotate("64.86.148.128", nil)
}

func TestCorrupt(t *testing.T) {
	setUp()
	ctx := context.Background()
	err := site.LoadFrom(ctx, corruptFile, corruptFile)
	if err == nil {
		t.Error("Expected load error")
	}
}
