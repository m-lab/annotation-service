package downloader

import (
	"archive/zip"
	"bytes"
	"cloud.google.com/go/storage"
	"errors"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"

	"github.com/m-lab/annotation-service/parser"
)

// Creates list of IP address Nodes
func InitializeTable(ctx context.Context, GCSFolder, GCSFile string) ([]parser.IPNode, []parser.IPNode, []parser.LocationNode, error) {
	// IPv4 database
	var IPv4List []parser.IPNode
	// IPv6 database
	var IPv6List []parser.IPNode
	// Location database
	var LocationList []parser.LocationNode

	if ctx == nil {
		ctx = context.Background()
	}
	zipReader, err := createReader(GCSFolder, GCSFile, ctx)
	if err != nil {
		log.Println(err)
		return IPv4List, IPv6List, LocationList, errors.New("Failed creating zipReader")
	}
	IPv4List, IPv6List, LocationList, err = parser.Unzip(zipReader)
	if err != nil {
		log.Println(err)
		return IPv4List, IPv6List, LocationList, errors.New("Failed Unzipping and creating lists")
	}
	return IPv4List, IPv6List, LocationList, nil
}

// Creates a zip.Reader 
func createReader(bucket string, bucketObj string, ctx context.Context) (*zip.Reader, error) {
	ctx = context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Println(err)
		return nil, errors.New("Failed creating new client")
	}
	obj := client.Bucket(bucket).Object(bucketObj)

	// Takes context returns *Reader
	reader, err := obj.NewReader(ctx)
	if err != nil {
		log.Println(err)
		return nil, errors.New("Failed creating new reader") 
	}
	bytesSlice, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Println(err)
		return nil, errors.New("Failed to create byte slice")
	}

	// Takes byte slice returns Reader
	r := bytes.NewReader(bytesSlice)

	// Takes r io.ReaderAt(implements Reader) and size of bytes. returns *Reader
	zipReader, err := zip.NewReader(r, int64(len(bytesSlice)))

	return zipReader, nil
}
