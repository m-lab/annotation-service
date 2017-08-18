package loader

import (
	"archive/zip"
	"bytes"
	"cloud.google.com/go/storage"
	"errors"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
)

// CreateZipReader reads a file from GCS and wraps it in a zip.Reader.
func CreateZipReader(ctx context.Context, bucket string, bucketObj string) (*zip.Reader, error) {
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
	if err != nil {
		log.Println(err)
		return nil, errors.New("Failed to create zip.Reader")
	}
	return zipReader, nil
}
