package downloader

import (
	"cloud.google.com/go/storage"
	"errors"
	"golang.org/x/net/context"
	//"log"
	"archive/zip"
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/m-lab/annotation-service/parser"
)

//Creates list of IP address Nodes
func InitializeTable(ctx context.Context, GCSFolder, GCSFile string) ([]parser.BlockNode, []parser.BlockNode, []parser.LocNode, error) {
	var listIPv4 []parser.BlockNode
	var listIPv6 []parser.BlockNode
	var listLoc []parser.LocNode

	if ctx == nil {
		ctx = context.Background()
	}
	zipReader := createReader(GCSFolder, GCSFile, ctx)
	if zipReader == nil {
		return listIPv4, listIPv6, listLoc, errors.New("failed creating zipReader")
	}
	listIPv4, listIPv6, listLoc, err := parser.Unzip(zipReader)
	if err != nil {
		return listIPv4, listIPv6, listLoc, errors.New("failed Unzipping and creating lists")
	}
	return listIPv4, listIPv6, listLoc, nil
}

//creates generic reader
func createReader(bucket string, bucketObj string, ctx context.Context) *zip.Reader {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil
	}
	obj := client.Bucket(bucket).Object(bucketObj)
	//takes context returns *Reader
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil
	}
	bytesSlice, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil
	}
	//takes byte slice returns Reader
	r := bytes.NewReader(bytesSlice)

	//takes r io.ReaderAt(implements Reader) and size of bytes. returns *Reader
	zipReader, err := zip.NewReader(r, int64(len(bytesSlice)))

	for _, f := range zipReader.File {
		fmt.Println(f.Name)
	}
	fmt.Println("good")
	return zipReader
}
