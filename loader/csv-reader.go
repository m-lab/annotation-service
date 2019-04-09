package loader

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
)

// Loader errors
var (
	ErrBadRecord     = errors.New("Corrupted Data: wrong number of columns")
	ErrTooFewColumns = errors.New("Header has too few columns")
	ErrTooManyErrors = errors.New("Too many errors during loading the dataset IP list")
)

var (
	maxFieldErrorsPerFile = 50 // the default maximum number of field errors per file
	maxBadRecordsPerFile  = 0  // the default maximum number of wrong records per file
)

// CSVReader reads the CSV and uses the CSVRecordConsumer interface to coordinate
// the parsing. Counts the errors and exits when the read fails.
type CSVReader struct {
	MaxBadRecordsPerFile  int
	MaxFieldErrorsPerFile int
	consumer              CSVRecordConsumer
	csvReader             *csv.Reader
}

// CSVRecordConsumer interface enables the abstraction of loading CSV files
type CSVRecordConsumer interface {
	PreconfigureReader(reader *csv.Reader) error // should customize the CSV reader to the datasource-specific format
	ValidateRecord(record []string) error        // should validate the raw CSV record
	Consume(record []string) error               // should collect the results of the validated line
}

// NewCSVReader initializes a new CSV reader
func NewCSVReader(reader io.Reader, consumer CSVRecordConsumer) CSVReader {
	newReader := CSVReader{
		MaxBadRecordsPerFile:  maxBadRecordsPerFile,
		MaxFieldErrorsPerFile: maxFieldErrorsPerFile,
		consumer:              consumer}
	csvReader := csv.NewReader(reader)
	newReader.csvReader = csvReader
	return newReader
}

// ReadAll reads all rows of the CSV file, validating records and fields as it goes.
// Returns ErrorTooManyErrors if there are too many record or field errors.
func (r *CSVReader) ReadAll() error {
	err := r.consumer.PreconfigureReader(r.csvReader)
	if err != nil {
		log.Println(err)
		return err
	}

	fieldErrors := 0
	badRecords := 0
	for {
		record, err := r.csvReader.Read()
		if err == io.EOF {
			break
		}

		err = r.consumer.ValidateRecord(record)
		if err != nil {
			log.Println(err)
			badRecords++
			if badRecords > r.MaxBadRecordsPerFile {
				return ErrBadRecord
			}
			continue
		}

		err = r.consumer.Consume(record)
		if err != nil {
			log.Println(err)
			fieldErrors++
			if fieldErrors > r.MaxFieldErrorsPerFile {
				return ErrTooManyErrors
			}
			continue
		}
	}
	return nil
}
