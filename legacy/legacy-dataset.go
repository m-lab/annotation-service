package legacy

/* From 2013/08/28 - 2017/08/08, Maxmind provide GeoLite dataset in legacy format

gs://downloader-mlab-oti/Maxmind/2013/08/28/20130828T184800Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2013/09/07/20130907T170000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2013/10/07/20131007T170000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2013/11/07/20131107T160000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2013/12/07/20131207T160000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/02/07/20140207T160000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/04/07/20140407T170000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/05/04/20140504T070800Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/05/08/20140508T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/06/08/20140608T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/07/08/20140708T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/08/08/20140808T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/09/08/20140908T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/10/28/20141028T032100Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/11/08/20141108T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2014/12/08/20141208T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/01/08/20150108T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/02/08/20150208T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/03/08/20150308T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/04/08/20150408T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/05/08/20150508T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/06/08/20150608T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/07/08/20150708T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/08/08/20150808T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/09/08/20150908T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/10/08/20151008T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/11/03/20151103T204100Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/11/08/20151108T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2015/12/08/20151208T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/01/08/20160108T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/02/08/20160208T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/03/08/20160308T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/04/08/20160408T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/05/08/20160508T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/06/08/20160608T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/07/08/20160708T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/08/08/20160808T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/09/08/20160908T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/10/08/20161008T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/11/08/20161108T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2016/12/08/20161208T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2017/01/08/20170108T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2017/02/08/20170208T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2017/03/08/20170308T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2017/04/08/20170408T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2017/05/08/20170508T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2017/06/08/20170608T080000Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2017/07/05/20170705T153500Z-GeoLiteCity.dat.gz
gs://downloader-mlab-oti/Maxmind/2017/08/08/20170808T080000Z-GeoLiteCity.dat.gz

   The first dataset (2013/08/28) cover all requests earlier than this date.
   Each data set cover the time range from its stamp to next availalbe dataset.
   There are IP v6 datasets as well.

   From 2017/08/15 - present, Maxmind provides both legacy format and GeoLite2

gs://downloader-mlab-oti/Maxmind/2017/08/15/20170815T200728Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/08/15/20170815T200946Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/09/01/20170901T004438Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/09/01/20170901T085053Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/09/07/20170907T023620Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/09/07/20170907T030659Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/10/01/20171001T003046Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/10/01/20171001T025802Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/10/04/20171004T191825Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/10/05/20171005T033334Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/10/05/20171005T040958Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/11/01/20171101T013013Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/11/06/20171106T232541Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/12/01/20171201T074609Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/12/01/20171201T183227Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2017/12/06/20171206T205411Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2018/01/01/20180101T033908Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2018/01/05/20180105T203044Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2018/02/01/20180201T045126Z-GeoLite2-City-CSV.zip
gs://downloader-mlab-oti/Maxmind/2018/02/08/20180208T013555Z-GeoLite2-City-CSV.zip
...


*/
import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/loader"
)

// This is the regex used to filter for which files we want to consider acceptable for using with legacy dataset
var GeoLegacyRegex = regexp.MustCompile(`.*-GeoLiteCity.dat.*`)
var GeoLegacyv6Regex = regexp.MustCompile(`.*-GeoLiteCityv6.dat.*`)

type LegacyDatasets struct {
	v4Data *GeoIP
	v6Data *GeoIP
}

// LoadBundleLegacyDataset loads both IPv4 and IPv6 version of the requested dataset into memory.
func LoadBundleLegacyDataset(filename string, bucketname string) (LegacyDatasets, error) {
	if GeoLegacyRegex.MatchString(filename) {
		v4, err := LoadLegacyGeoliteDataset(filename, bucketname)
		if err != nil {
			return LegacyDatasets{nil, nil}, errors.New("cannot load IPv4 dataset")
		}
		v6, err := LoadLegacyGeoliteDataset(strings.Replace(filename, "GeoLiteCity", "GeoLiteCityv6", -1), bucketname)
		if err != nil {
			return LegacyDatasets{nil, nil}, errors.New("cannot load IPv6 dataset")
		}
		return LegacyDatasets{v4Data: v4, v6Data: v6}, nil
	}

	if GeoLegacyv6Regex.MatchString(filename) {
		v6, err := LoadLegacyGeoliteDataset(filename, bucketname)
		if err != nil {
			return LegacyDatasets{nil, nil}, errors.New("cannot load IPv6 dataset")
		}
		v4, err := LoadLegacyGeoliteDataset(strings.Replace(filename, "GeoLiteCityv6", "GeoLiteCity", -1), bucketname)
		if err != nil {
			return LegacyDatasets{nil, nil}, errors.New("cannot load IPv4 dataset")
		}
		return LegacyDatasets{v4Data: v4, v6Data: v6}, nil
	}

	return LegacyDatasets{nil, nil}, errors.New("Wrong input dataset name")
}

// LoadGeoliteDataset will check GCS for the matching dataset, download
// it, process it, and load it into memory so that it can be easily
// searched, then it will return a pointer to that GeoDataset or an error.
func LoadLegacyGeoliteDataset(filename string, bucketname string) (*GeoIP, error) {
	// load the legacy binary dataset
	dataFileName := "GeoLiteCity.dat"
	err := loader.UncompressGzFile(context.Background(), bucketname, filename, dataFileName)
	if err != nil {
		return nil, err
	}
	gi, err := Open(dataFileName, filename)
	if err != nil {
		return nil, errors.New("could not open GeoIP database")
	}
	return gi, nil
}

func round(x float32) float64 {
	i, err := strconv.ParseFloat(fmt.Sprintf("%.3f", x), 64)
	if err != nil {
		return float64(0)
	}
	return i
}

func GetRecordFromLegacyDataset(ip string, gi LegacyDatasets, isIP4 bool) *api.GeoData {
	if gi.v4Data == nil || gi.v6Data == nil {
		return nil
	}
	var record *GeoIPRecord
	if isIP4 {
		record = gi.v4Data.GetRecord(ip, isIP4)
	} else {
		record = gi.v6Data.GetRecord(ip, isIP4)
	}

	// It is very possible that the record missed some fields in legacy dataset.
	if record != nil {
		return &api.GeoData{
			Geo: &api.GeolocationIP{
				ContinentCode: record.ContinentCode,
				CountryCode:   record.CountryCode,
				CountryCode3:  record.CountryCode3,
				CountryName:   record.CountryName,
				Region:        record.Region,
				MetroCode:     int64(record.MetroCode),
				City:          record.City,
				AreaCode:      int64(record.AreaCode),
				PostalCode:    record.PostalCode,
				Latitude:      round(record.Latitude),
				Longitude:     round(record.Longitude),
			},
			ASN: &api.IPASNData{},
		}
	}
	return nil
}