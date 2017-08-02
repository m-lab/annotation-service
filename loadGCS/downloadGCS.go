package loadGCS

import (
	"golang.org/x/net/context"
	"cloud.google.com/go/storage"
	"log"
	"os"

	"github.com/m-lab/annotation-service/createList"
)

var geoData []createList.Node

func InitializeTable(ctx context.Context, GCSFolder,GCSFile string) {

	if ctx == nil {
		ctx = context.Background()
	}
	
	storageReader, err := createReader(GCSFolder,GCSFile,ctx) 
	//storageReader, err := createReader("test-annotator-sandbox", "annotator-data/GeoIPCountryWhois.csv", ctx)
	if err != nil {
		return
	}

	geoData, err = createList.CreateList(storageReader)
	if err != nil {
		log.Println("geoData createList failed")
		os.Exit(1)
	}
}
//creates generic reader
func createReader(bucket string, bucketObj string, ctx context.Context) (*storage.Reader, error) {

	client, err := storage.NewClient(ctx)

	if err != nil {
		log.Fatal(err)
	}

	bkt := client.Bucket(bucket)

	obj := bkt.Object(bucketObj)
	reader, err := obj.NewReader(ctx)

	if err != nil {
		log.Fatal(err)
	}
	return reader, nil

}
