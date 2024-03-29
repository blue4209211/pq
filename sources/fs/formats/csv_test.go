package formats

import (
	"strings"
	"testing"

	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestCSVDataSourceName(t *testing.T) {
	source := CsvDataSource{}
	assert.Equal(t, source.Name(), "csv")
}

func TestCSVDataSourceReader(t *testing.T) {
	source := CsvDataSource{}

	csvString :=
		`a,b,c,d
1,2,"c1","d1"
3,4,"c2","d,2"
5,,"","d2"
`

	csvReader, err := source.Reader(strings.NewReader(csvString), map[string]string{})
	assert.NoError(t, err)

	schema := csvReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, schema.Len())
	assert.Equal(t, "a", schema.Get(0).Name)
	assert.Equal(t, "string", schema.Get(0).Format.Name())
	assert.Equal(t, "b", schema.Get(1).Name)
	assert.Equal(t, "string", schema.Get(1).Format.Name())
	assert.Equal(t, "c", schema.Get(2).Name)
	assert.Equal(t, "string", schema.Get(2).Format.Name())
	assert.Equal(t, "d", schema.Get(3).Name)
	assert.Equal(t, "string", schema.Get(3).Format.Name())
	data := *(csvReader.Data())
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, "1", data[0].GetRaw(0))
	assert.Equal(t, "2", data[0].GetRaw(1))
	assert.Equal(t, "c1", data[0].GetRaw(2))
	assert.Equal(t, "d1", data[0].GetRaw(3))

	assert.Equal(t, "", data[2].GetRaw(1))
	assert.Equal(t, "", data[2].GetRaw(2))

}

func TestCSVDataSourceReaderNoHeaderDifferentSep(t *testing.T) {
	source := CsvDataSource{}

	csvString :=
		`1	2	"c1"	"d1"
3	4	"c2"	"d	2"
5	""	""	"d2"
`

	csvReader, err := source.Reader(strings.NewReader(csvString), map[string]string{
		ConfigCsvHeader: "false",
		ConfigCsvSep:    "\t",
	})
	assert.NoError(t, err)

	schema := csvReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, schema.Len())
	assert.Equal(t, "c0", schema.Get(0).Name)
	assert.Equal(t, "string", schema.Get(0).Format.Name())
	assert.Equal(t, "c1", schema.Get(1).Name)
	assert.Equal(t, "string", schema.Get(1).Format.Name())
	assert.Equal(t, "c2", schema.Get(2).Name)
	assert.Equal(t, "string", schema.Get(2).Format.Name())
	assert.Equal(t, "c3", schema.Get(3).Name)
	assert.Equal(t, "string", schema.Get(3).Format.Name())
	data := csvReader.Data()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(*data))

	// wrong seprator
	_, err = source.Reader(strings.NewReader(csvString), map[string]string{
		ConfigCsvHeader: "false",
		ConfigCsvSep:    "^^",
	})
	assert.Error(t, err)
}

func TestCSVDataSourceWriter(t *testing.T) {
	source := CsvDataSource{}

	csvString :=
		`1	2	c1	d1
3	4	c2	"d	2"
5			d2
`

	configs := map[string]string{
		ConfigCsvHeader: "false",
		ConfigCsvSep:    "\t",
	}
	csvReader, err := source.Reader(strings.NewReader(csvString), configs)
	assert.NoError(t, err)
	dataframe := inmemory.NewDataframeFromRowAndName("df_1", csvReader.Schema(), csvReader.Data())
	writer, err := source.Writer(dataframe, configs)
	assert.Nil(t, err)
	buff := new(strings.Builder)
	writer.Write(buff)

	assert.Equal(t, csvString, buff.String())

	csvStringWithHeader := `a	b	c	d
1	2	c1	d1
3	4	c2	"d	2"
5			d2
`
	configs = map[string]string{
		ConfigCsvHeader: "true",
		ConfigCsvSep:    "\t",
	}
	csvReader, err = source.Reader(strings.NewReader(csvStringWithHeader), configs)
	assert.NoError(t, err)
	dataframe = inmemory.NewDataframeFromRowAndName("df_1", csvReader.Schema(), csvReader.Data())
	writer, err = source.Writer(dataframe, configs)
	assert.Nil(t, err)
	buff = new(strings.Builder)
	writer.Write(buff)

	assert.Equal(t, csvStringWithHeader, buff.String())

}

func BenchmarkCSVParsing(b *testing.B) {

	source := CsvDataSource{}
	csvString := `1,2,"c1","d1"
3,4,"c2","d,2"
5,,"","d2"`

	csvStringData := csvString
	for i := 1; i < 1000; i++ {
		csvStringData = csvStringData + "\n" + csvString
	}

	b.Run("perf-alldata", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			csvReader, _ := source.Reader(strings.NewReader(csvStringData), map[string]string{})
			csvReader.Schema()
			csvReader.Data()
		}
	})

}
