package dataset

import (
	"errors"
	"log"
	"os"
	"sync"

	"github.com/m-lab/annotation-service/common"
	"github.com/m-lab/annotation-service/geoip"
	"github.com/m-lab/annotation-service/parser"
)

const (
	// Maximum number of Geolite2 datasets in memory.
	MaxHistoricalGeolite2Dataset = 5
	// Maximum number of legacy datasets in memory.
	// IPv4 and IPv6 are separated for legacy datasets.
	MaxHistoricalLegacyDataset = 6
	// Maximum number of pending datasets that can be loaded at the same time.
	MaxPendingDataset = 2
)

var (

	// This is the bucket containing maxmind files.
	BucketName = "downloader-" + os.Getenv("GCLOUD_PROJECT")

	// The list of dataset that was loading right now.
	// Due to memory limits, the length of PendingDataset should not exceed 2.
	PendingDataset = []string{}

	// channel to protect PendingDataset
	PendingMutex = &sync.RWMutex{}
)

func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func Deletes(a []string, x string) []string {
	for i, v := range a {
		if v == x {
			copy(a[i:], a[i+1:])
			a[len(a)-1] = ""
			a = a[:len(a)-1]
			return a
		}
	}
	return a
}

// searchGeoLocation is interface that handle the dataset related operations
type searchGeoLocation interface {
	GetGeoLocationForSingleIP(request *common.RequestData, filename string) (*common.GeoData, error)
	AddDataset(filename string)
	Init()
}

// CurrentDatasetInMemory is struct that handling the latest dataset in memory.
type CurrentDatasetInMemory struct {
	sync.RWMutex
	current *parser.GeoDataset
}

func (d *CurrentDatasetInMemory) Init() {
	d.current = nil
}

func (d *CurrentDatasetInMemory) AddDataset(filename string) {
	d.Lock()
	parser, err := LoadGeoLite2Dataset(filename, BucketName)
	if err != nil {
		log.Fatal(err)
	}
	d.current = parser
	d.Unlock()
}

func (d *CurrentDatasetInMemory) GetDataset(filename string) *parser.GeoDataset {
	d.RLock()
	defer d.RUnlock()
	return d.current
}

func (d *CurrentDatasetInMemory) GetGeoLocationForSingleIP(request *common.RequestData, filename string) (*common.GeoData, error) {
	return UseGeoLite2Dataset(request, d.GetDataset(""))
}

func (d *CurrentDatasetInMemory) SetDataset(p *parser.GeoDataset) {
	d.Lock()
	d.current = p
	d.Unlock()
}

// LegacyDatasetInMemory handles all legacy datasets in memory.
type LegacyDatasetInMemory struct {
	sync.RWMutex
	legacyData map[string]*geoip.GeoIP
}

func (d *LegacyDatasetInMemory) Init() {
	d.Lock()
	d.legacyData = make(map[string]*geoip.GeoIP)
	d.Unlock()
}

func (d *LegacyDatasetInMemory) AddDataset(filename string) {
	// check whether this dataset is already in memory
	if d.legacyData[filename] != nil {
		return
	}

	d.Lock()
	// Add a new legacy dataset
	if len(d.legacyData) >= MaxHistoricalLegacyDataset {
		// Remove one entry
		for key, _ := range d.legacyData {
			log.Println("remove legacy dataset " + key)
			d.legacyData[key].Free()
			delete(d.legacyData, key)
			break
		}
	}
	parser, err := LoadLegacyGeoliteDataset(filename, BucketName)
	if err != nil {
		//return nil, errors.New("Cannot load historical dataset into memory " + filename)
		return
	}
	log.Println("historical legacy dataset loaded " + filename)
	d.legacyData[filename] = parser
	log.Printf("number of legacy dataset in memory: %d ", len(d.legacyData))
	log.Println(d.legacyData)
	d.Unlock()
}

func (d *LegacyDatasetInMemory) GetDataset(filename string) *geoip.GeoIP {
	d.RLock()
	defer d.RUnlock()
	return d.legacyData[filename]
}

func (d *LegacyDatasetInMemory) GetGeoLocationForSingleIP(request *common.RequestData, filename string) (*common.GeoData, error) {
	isIP4 := true
	if request.IPFormat == 6 {
		isIP4 = false
	}

	if parser := d.GetDataset(filename); parser != nil {
		return GetRecordFromLegacyDataset(request.IP, parser, isIP4), nil
	}
	PendingMutex.Lock()
	if Contains(PendingDataset, filename) {
		PendingMutex.Unlock()
		// dataset loading, just return.
		return nil, errors.New("Historical dataset is loading into memory right now " + filename)
	}
	if len(PendingDataset) >= MaxPendingDataset {
		PendingMutex.Unlock()
		return nil, errors.New("Too many pending loading right now, cannot load " + filename)
	}
	PendingDataset = append(PendingDataset, filename)
	d.AddDataset(filename)
	PendingDataset = Deletes(PendingDataset, filename)
	PendingMutex.Unlock()

	return GetRecordFromLegacyDataset(request.IP, d.GetDataset(filename), isIP4), nil
}

// Geolite2DatasetInMemory handles all Geolite2 datasets in memory.
type Geolite2DatasetInMemory struct {
	sync.RWMutex
	geolite2Data map[string]*parser.GeoDataset
}

func (d *Geolite2DatasetInMemory) Init() {
	d.Lock()
	d.geolite2Data = make(map[string]*parser.GeoDataset)
	d.Unlock()
}

func (d *Geolite2DatasetInMemory) AddDataset(filename string) {
	// check whether this dataset is already in memory
	if d.geolite2Data[filename] != nil {
		return
	}

	d.Lock()
	// Add a new legacy dataset
	if len(d.geolite2Data) >= MaxHistoricalGeolite2Dataset {
		// Remove one entry
		for key, _ := range d.geolite2Data {
			log.Println("remove Geolite2 dataset " + key)
			//d.geolite2Data[key].Free()
			delete(d.geolite2Data, key)
			break
		}
	}
	parser, err := LoadGeoLite2Dataset(filename, BucketName)
	if err != nil {
		//return nil, errors.New("Cannot load Geolite2 dataset into memory " + filename)
		return
	}
	log.Println("historical Geolite2 dataset loaded " + filename)
	d.geolite2Data[filename] = parser
	log.Printf("number of Geolite2 dataset in memory: %d ", len(d.geolite2Data))
	log.Println(d.geolite2Data)
	d.Unlock()
}

func (d *Geolite2DatasetInMemory) GetDataset(filename string) *parser.GeoDataset {
	d.RLock()
	defer d.RUnlock()
	return d.geolite2Data[filename]
}

func (d *Geolite2DatasetInMemory) GetGeoLocationForSingleIP(request *common.RequestData, filename string) (*common.GeoData, error) {
	if parser := d.GetDataset(filename); parser != nil {
		return UseGeoLite2Dataset(request, parser)
	}
	PendingMutex.Lock()
	if Contains(PendingDataset, filename) {
		PendingMutex.Unlock()
		// dataset loading, just return.
		return nil, errors.New("Historical dataset is loading into memory right now " + filename)
	}
	if len(PendingDataset) >= MaxPendingDataset {
		PendingMutex.Unlock()
		return nil, errors.New("Too many pending loading right now, cannot load " + filename)
	}
	PendingDataset = append(PendingDataset, filename)
	d.AddDataset(filename)
	PendingDataset = Deletes(PendingDataset, filename)
	PendingMutex.Unlock()

	return UseGeoLite2Dataset(request, d.GetDataset(filename))
}
