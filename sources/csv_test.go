package sources

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCSVDataSourceName(t *testing.T) {
	source := csvDataSource{}
	assert.Equal(t, source.Name(), "csv")
}

func TestCSVDataSourceReader(t *testing.T) {
	source := csvDataSource{}

	csvString :=
		`a,b,c,d
1,2,"c1","d1"
3,4,"c2","d,2"
5,,"","d2"
`

	csvReader, err := source.Reader(strings.NewReader(csvString), map[string]string{})
	assert.NoError(t, err)

	schema, err := csvReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, len(schema))
	assert.Equal(t, "a", schema[0].Name)
	assert.Equal(t, "string", schema[0].Format.Name())
	assert.Equal(t, "b", schema[1].Name)
	assert.Equal(t, "string", schema[1].Format.Name())
	assert.Equal(t, "c", schema[2].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	assert.Equal(t, "d", schema[3].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	data, err := csvReader.Data()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, "1", data[0][0])
	assert.Equal(t, "2", data[0][1])
	assert.Equal(t, "c1", data[0][2])
	assert.Equal(t, "d1", data[0][3])

	assert.Equal(t, "", data[2][1])
	assert.Equal(t, "", data[2][2])

}

func TestCSVDataSourceReaderNoHeaderDifferentSep(t *testing.T) {
	source := csvDataSource{}

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

	schema, err := csvReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, len(schema))
	assert.Equal(t, "c0", schema[0].Name)
	assert.Equal(t, "string", schema[0].Format.Name())
	assert.Equal(t, "c1", schema[1].Name)
	assert.Equal(t, "string", schema[1].Format.Name())
	assert.Equal(t, "c2", schema[2].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	assert.Equal(t, "c3", schema[3].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	data, err := csvReader.Data()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))

	// wrong seprator
	csvReader, err = source.Reader(strings.NewReader(csvString), map[string]string{
		ConfigCsvHeader: "false",
		ConfigCsvSep:    "^^",
	})
	assert.NoError(t, err)

	schema, err = csvReader.Schema()
	assert.Error(t, err)
}

func TestCSVDataSourceWriter(t *testing.T) {
	source := csvDataSource{}

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
	dataframe := NewDatasourceDataFrame("df_1", csvReader)
	writer, err := source.Writer(&dataframe, configs)
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
	dataframe = NewDatasourceDataFrame("df_1", csvReader)
	writer, err = source.Writer(&dataframe, configs)
	buff = new(strings.Builder)
	writer.Write(buff)

	assert.Equal(t, csvStringWithHeader, buff.String())

}

func BenchmarkCSVParsing(b *testing.B) {

	source := csvDataSource{}
	csvString := `1,2,"c1","d1"
3,4,"c2","d,2"
5,,"","d2"`

	csvStringData := csvString
	for i := 1; i < 1000; i++ {
		csvStringData = csvStringData + "\n" + csvString
	}

	for i := 0; i < b.N; i++ {
		csvReader, _ := source.Reader(strings.NewReader(csvStringData), map[string]string{})
		csvReader.Schema()
		csvReader.Data()
	}

}
