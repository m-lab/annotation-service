package handler

import (
	"log"
	"sync"

	"github.com/m-lab/annotation-service/handler/geoip"
	"github.com/m-lab/annotation-service/parser"
)

type DatasetInMemory struct {
	sync.RWMutex
	current    *parser.GeoDataset
	data       map[string]*parser.GeoDataset
	legacyData map[string]*geoip.GeoIP
}

func (d *DatasetInMemory) Init() {
	d.current = nil
	d.data = make(map[string]*parser.GeoDataset)
	d.legacyData = make(map[string]*geoip.GeoIP)
}

// This func will make the data map size to 1 and contains only the current dataset.
func (d *DatasetInMemory) SetCurrentDataset(inputData *parser.GeoDataset) {
	d.Lock()
	d.current = inputData
	d.Unlock()
}

func (d *DatasetInMemory) GetCurrentDataset() *parser.GeoDataset {
	d.RLock()
	defer d.RUnlock()
	return d.current
}

func (d *DatasetInMemory) AddDataset(filename string, inputData *parser.GeoDataset) {
	d.Lock()
	if len(d.data) >= MaxHistoricalGeolite2Dataset {
		// Remove one entry
		for key, _ := range d.data {
			log.Println("remove Geolite2 dataset " + key)
			d.data[key].Free()
			delete(d.data, key)
			break
		}
	}
	d.data[filename] = inputData
	log.Println(d.data)
	log.Printf("number of dataset in memory: %d ", len(d.data))
	d.Unlock()
}

func (d *DatasetInMemory) GetDataset(filename string) *parser.GeoDataset {
	d.RLock()
	defer d.RUnlock()
	return d.data[filename]
}

func (d *DatasetInMemory) GetLegacyDataset(filename string) *geoip.GeoIP {
	d.RLock()
	defer d.RUnlock()
	return d.legacyData[filename]
}

func (d *DatasetInMemory) AddLegacyDataset(filename string, inputData *geoip.GeoIP) {
	d.Lock()
	if len(d.legacyData) >= MaxHistoricalLegacyDataset {
		// Remove one entry
		for key, _ := range d.legacyData {
			log.Println("remove legacy dataset " + key)
			d.legacyData[key].Free()
			delete(d.legacyData, key)
			break
		}
	}
	d.legacyData[filename] = inputData
	log.Printf("number of legacy dataset in memory: %d ", len(d.legacyData))
	d.Unlock()
}
