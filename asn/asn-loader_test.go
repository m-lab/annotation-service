package asn

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/go/rtx"
)

func TestLoadASNDatasetFromReader(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		ip      string
		want    uint32
		wantErr bool
	}{
		{
			name:   "success-ipv4",
			source: "testdata/RouteViewIPv4.pfx2as",
			ip:     "1.0.0.1",
			want:   13335,
		},
		{
			name:   "success-ipv6",
			source: "testdata/RouteViewIPv6.pfx2as",
			ip:     "2001:4860:4860::8888",
			want:   15169,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			b, err := ioutil.ReadFile(tt.source)
			rtx.Must(err, "Failed to load source file")

			r := bytes.NewBuffer(b)

			asnReader, err := LoadASNDatasetFromReader(r)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadASNDatasetFromReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			ann := &api.Annotations{}
			err = asnReader.Annotate(tt.ip, ann)
			if (err != nil) != tt.wantErr {
				t.Errorf("Annotate error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if ann.Network.Systems[0].ASNs[0] != tt.want {
				t.Errorf("Annotate assigned ASN = %d, want %d", ann.Network.Systems[0].ASNs[0], tt.want)
			}
		})
	}
}
