package loader

import (
	"archive/zip"
	"bytes"
	"cloud.google.com/go/storage"
	"compress/gzip"
	"errors"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"log"
	"strings"
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

// Field "fn" is the filename being searched for within the zip file
// f should be closed when we load the data into the memory.
func FindFile(fn string, zrdr *zip.Reader) (io.ReadCloser, error) {
	for _, f := range zrdr.File {
		if strings.HasSuffix(f.Name, fn) {
			rc, err := f.Open()
			if err != nil {
				return nil, err
			}
			return rc, nil
		}
	}
	log.Println("File not found")
	return nil, errors.New("File not found")
}

// UncompressGzFile reads a .gz file from GCS and write it to a local file.
func UncompressGzFile(ctx context.Context, bucketName string, fileName string, outputFile string) error {
	ctx = context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Println(err)
		return errors.New("Failed creating new client")
	}

	// Creates a Bucket instance.
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(fileName).ReadCompressed(true)

	rdr, err := obj.NewReader(ctx)
	if err != nil {
		log.Fatal(err)
		return err
	}

	gzr, err := gzip.NewReader(rdr)
	if err != nil {
		log.Fatal(err)
		return err
	}
	data, err := ioutil.ReadAll(gzr)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(outputFile, data, 0644)
	return err
}
