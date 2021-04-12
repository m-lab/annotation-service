package legacy

import (
	"reflect"
	"testing"
)

func Test_parseFips2ISOMap(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     map[string]subdivision
		wantErr  bool
	}{
		{
			name:     "success",
			filename: "testdata/fips-iso-map-test.csv",
			want: map[string]subdivision{
				"US-UT": subdivision{"UT", "Utah"},
				"US-VT": subdivision{"VT", "Vermont"},
				"US-VA": subdivision{"VA", "Virginia"},
			},
		},
		{
			name:     "error-file-does-not-exist",
			filename: "nodir/file-does-not-exist.csv",
			wantErr:  true,
		},
		{
			name:     "error-file-empty",
			filename: "testdata/empty-file.csv",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFips2ISOMap(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseFips2ISOMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseFips2ISOMap() = %v, want %v", got, tt.want)
			}
		})
	}
}
