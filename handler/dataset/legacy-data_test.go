package dataset_test

import (
	"fmt"
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
	filename, err := dataset.SelectGeoLegacyFile(20110203)
	if filename != "Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz", filename, err)
	}

	filename2, err := dataset.SelectGeoLegacyFile(20140203)
	if filename2 != "Maxmind/2014/02/07/20140207T160000Z-GeoLiteCity.dat.gz" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2014/02/07/20140207T160000Z-GeoLiteCity.dat.gz", filename2, err)
	}

	filename3, err := dataset.SelectGeoLegacyFile(20170809)
	if filename3 != "Maxmind/2017/08/15/20170815T200728Z-GeoLite2-City-CSV.zip" || err != nil {
		t.Errorf("Did not select correct dataset. Expected %s, got %s, %+v.",
			"Maxmind/2017/08/15/20170815T200728Z-GeoLite2-City-CSV.zip", filename3, err)
	}
}

func TestLoadLegacyGeoliteDataset(t *testing.T) {
	gi, err := dataset.LoadLegacyGeoliteDataset(20140203)
	fmt.Printf("%v", err)
	if gi != nil {
		record := gi.GetRecord("207.171.7.51")
		fmt.Printf("%v\n", record)
	}
}
