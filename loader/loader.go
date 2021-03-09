// Package loader has tools for finding, reading, and uncompressing gzip files.
// The UncompressGzFile is required for legacy MaxMind data used by the external MaxMind library.
package loader

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

// GetGzBase extracts basename, such as "20140307T160000Z-GeoLiteCity.dat"
// from "Maxmind/2014/03/07/20140307T160000Z-GeoLiteCity.dat.gz"
// KZ: moved from legacy package here
func GetGzBase(filename string) string {
	base := filepath.Base(filename)
	return base[0 : len(base)-3]
}

// CreateZipReader reads a file from GCS and wraps it in a zip.Reader.
func CreateZipReader(ctx context.Context, bucket string, bucketObj string) (*zip.Reader, error) {
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
	defer reader.Close()
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

// FindFile searches through the zip file for the filen named fn.
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
// Consumer should delete the file when finished.
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
		log.Println(err)
		return err
	}

	gzr, err := gzip.NewReader(rdr)
	if err != nil {
		log.Println(err)
		return err
	}
	// TODO - this could be done incrementally, to use less memory.
	data, err := ioutil.ReadAll(gzr)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(outputFile, data, 0644)
	return err
}
