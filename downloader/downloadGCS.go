package downloader

import (
	"golang.org/x/net/context"
	"cloud.google.com/go/storage"
	"log"

	"github.com/m-lab/annotation-service/parser"
)

var geoData []parser.Node

func InitializeTable(ctx context.Context, GCSFolder,GCSFile string) *[]parser.Node{

	if ctx == nil {
		ctx = context.Background()
	}
	storageReader, err := createReader(GCSFolder,GCSFile,ctx) 
	if err != nil {
		log.Println("storage reader failed")	
		return nil
	}
	geoData, err = parser.CreateList(storageReader)
	if err != nil {
		log.Println("geoData createList failed")
		return nil
	}
	return &geoData
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
