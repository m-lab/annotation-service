package dataset_test

import (
	"testing"

	"github.com/m-lab/annotation-service/handler/dataset"
)

func TestExtractDateFromFilename(t *testing.T) {
	date, err := dataset.ExtractDateFromFilename("Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz")
	if date != 20170508 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %d, %+v.", 20170508, date, err)
	}

	date2, err := dataset.ExtractDateFromFilename("Maxmind/2017/10/05/20171005T033334Z-GeoLite2-City-CSV.zip")
	if date2 != 20171005 || err != nil {
		t.Errorf("Did not extract data correctly. Expected %d, got %d, %+v.", 20171005, date2, err)
	}
}

func TestSelectGeoLegacyFile(t *testing.T) {
        
}
