package loader

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
)

var (
	maxWrongRecordsPerFile = 50 // the default maximum number of wrong records per file

	// ErrorTooManyErrors raised when the maximum number of errors during the import of a single file is > then maxWrongRecordsPerFile
	ErrorTooManyErrors = errors.New("Too many errors during loading the dataset IP list")
)

// CSVReader reads the CSV and uses the CSVRecordConsumer interface to coordinate
// the parsing. Counts the errors and exits when the read fails.
type CSVReader struct {
	MaxWrongRecordsPerFile int
	consumer               CSVRecordConsumer
	csvReader              *csv.Reader
}

// CSVRecordConsumer interface enables the abstraction of loading CSV files
type CSVRecordConsumer interface {
	PreconfigureReader(reader *csv.Reader) error // should customize the CSV reader to the datasource-specific format
	ValidateRecord(record []string) error        // should validate the raw CSV record
	Consume(record []string) error               // should collect the results of the validated line
}

// NewCSVReader initializes a new CSV reader
func NewCSVReader(reader io.Reader, consumer CSVRecordConsumer) CSVReader {
	newReader := CSVReader{MaxWrongRecordsPerFile: maxWrongRecordsPerFile, consumer: consumer}
	csvReader := csv.NewReader(reader)
	newReader.csvReader = csvReader
	return newReader
}

func (r *CSVReader) ReadAll() error {
	err := r.consumer.PreconfigureReader(r.csvReader)
	if err != nil {
		log.Println(err)
		return err
	}

	errorCount := 0
	for {
		record, err := r.csvReader.Read()
		if err == io.EOF {
			break
		}

		err = r.consumer.ValidateRecord(record)
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > r.MaxWrongRecordsPerFile {
				return ErrorTooManyErrors
			}
			continue
		}

		err = r.consumer.Consume(record)
		if err != nil {
			log.Println(err)
			errorCount++
			if errorCount > r.MaxWrongRecordsPerFile {
				return ErrorTooManyErrors
			}
			continue
		}
	}
	return nil
}
