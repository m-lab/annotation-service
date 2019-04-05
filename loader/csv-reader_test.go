package loader_test

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/m-lab/annotation-service/loader"
	"github.com/stretchr/testify/assert"
)

func TestCSVReaderOK(t *testing.T) {

	tabSeparatedCSV := `1.0.0.0/24	custom1
1.0.0.2/26	custom2
1.0.10.0/24	custom3
1.0.10.124/30	custom4
2.1.0.0/8	custom5`

	commaSeparatedCSV := `1.0.0.0/24,custom1
1.0.0.2/26,custom2
1.0.10.0/24,custom3
1.0.10.124/30,custom4
2.1.0.0/8,custom5`

	expected := []testCSVRecord{
		newTestCSVRecord("1.0.0.0/24", "custom1"),
		newTestCSVRecord("1.0.0.2/26", "custom2"),
		newTestCSVRecord("1.0.10.0/24", "custom3"),
		newTestCSVRecord("1.0.10.124/30", "custom4"),
		newTestCSVRecord("2.1.0.0/8", "custom5"),
	}

	// test with tab separated
	tsvStrReader := strings.NewReader(tabSeparatedCSV)
	tsvConsumer := newConsumerForTSV()
	tsvReader := loader.NewCSVReader(tsvStrReader, tsvConsumer)
	tsvReader.MaxBadRecordsPerFile = 0
	err := tsvReader.ReadAll()
	assert.Nil(t, err)
	assertEqual(t, expected, tsvConsumer.results)

	// test with coma separated
	csvStrReader := strings.NewReader(commaSeparatedCSV)
	csvConsumer := newConsumerForCSV()
	csvReader := loader.NewCSVReader(csvStrReader, csvConsumer)
	csvReader.MaxBadRecordsPerFile = 0
	err = csvReader.ReadAll()
	assert.Nil(t, err)
	assertEqual(t, expected, csvConsumer.results)
}

func TestFailsWhenMaxErrorCountReached(t *testing.T) {
	tabSeparatedCSV := `1.0.0.0/24	custom1
1.0.0.2/26	custom2
1.0.10.0/24ustom3
1.0.10.124/30	custom4	erert
2.1.0.0/8	custom5`

	expected := []testCSVRecord{
		newTestCSVRecord("1.0.0.0/24", "custom1"),
		newTestCSVRecord("1.0.0.2/26", "custom2"),
		newTestCSVRecord("2.1.0.0/8", "custom5"),
	}

	// no failure on 2 bad records, 2 error allowed
	tsvStrReader := strings.NewReader(tabSeparatedCSV)
	tsvConsumer := newConsumerForTSV()
	tsvReader := loader.NewCSVReader(tsvStrReader, tsvConsumer)
	tsvReader.MaxBadRecordsPerFile = 2
	err := tsvReader.ReadAll()
	assert.Nil(t, err)
	assertEqual(t, expected, tsvConsumer.results)

	// failuure when only 1 error allowed
	tsvStrReader = strings.NewReader(tabSeparatedCSV)
	tsvConsumer = newConsumerForTSV()
	tsvReader = loader.NewCSVReader(tsvStrReader, tsvConsumer)
	tsvReader.MaxBadRecordsPerFile = 1
	err = tsvReader.ReadAll()
	assert.EqualError(t, err, loader.ErrBadRecord.Error())
}

func assertEqual(t *testing.T, expected []testCSVRecord, got []testCSVRecord) {
	assert.Equal(t, len(expected), len(got))
	for idx, val := range expected {
		assert.Equal(t, val.ip, got[idx].ip)
		assert.Equal(t, val.custom, got[idx].custom)
	}
}

type testCSVRecord struct {
	ip     string
	custom string
}

func newTestCSVRecord(ip, custom string) testCSVRecord {
	return testCSVRecord{
		ip:     ip,
		custom: custom,
	}
}

type testCSVConsumer struct {
	isTsv   bool
	results []testCSVRecord
}

func newConsumerForTSV() *testCSVConsumer {
	return &testCSVConsumer{
		isTsv:   true,
		results: []testCSVRecord{},
	}
}

func newConsumerForCSV() *testCSVConsumer {
	return &testCSVConsumer{
		isTsv:   false,
		results: []testCSVRecord{},
	}
}

func (c *testCSVConsumer) PreconfigureReader(reader *csv.Reader) error {
	if c.isTsv {
		reader.Comma = '\t'
	} else {
		reader.Comma = ','
	}
	return nil
}

func (c *testCSVConsumer) ValidateRecord(record []string) error {
	if len(record) != 2 {
		return loader.ErrBadRecord
	}
	return nil
}

func (c *testCSVConsumer) Consume(record []string) error {
	c.results = append(c.results, newTestCSVRecord(record[0], record[1]))
	return nil
}
