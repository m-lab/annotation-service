package downloader

import (
	"archive/zip"
	"bytes"
	"cloud.google.com/go/storage"
	"errors"
	"golang.org/x/net/context"
	"io/ioutil"

	"github.com/m-lab/annotation-service/parser"
)

// Creates list of IP address Nodes
func InitializeTable(ctx context.Context, GCSFolder, GCSFile string) ([]parser.BlockNode, []parser.BlockNode, []parser.LocationNode, error) {
	// IPv4 database
	var IPv4List []parser.BlockNode
	// IPv6 database
	var IPv6List []parser.BlockNode
	// Location database 
	var LocationList []parser.LocationNode

	if ctx == nil {
		ctx = context.Background()
	}
	zipReader,err := createReader(GCSFolder, GCSFile, ctx)
	if err != nil {
		return IPv4List, IPv6List, LocationList, errors.New("Failed creating zipReader")
	}
	IPv4List, IPv6List, LocationList, err = parser.Unzip(zipReader)
	if err != nil {
		return IPv4List, IPv6List, LocationList, errors.New("Failed Unzipping and creating lists")
	}
	return IPv4List, IPv6List, LocationList, nil
}

// Creates generic reader
func createReader(bucket string, bucketObj string, ctx context.Context) (*zip.Reader,error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil,err
	}
	obj := client.Bucket(bucket).Object(bucketObj)

	// Takes context returns *Reader
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil,err
	}
	bytesSlice, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil,err
	}

	// Takes byte slice returns Reader
	r := bytes.NewReader(bytesSlice)

	// Takes r io.ReaderAt(implements Reader) and size of bytes. returns *Reader
	zipReader, err := zip.NewReader(r, int64(len(bytesSlice)))

	return zipReader,nil
}
