package engine

import (
	"reflect"
	"strings"
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/sources"
	"github.com/stretchr/testify/assert"
)

func TestQuerySingleCSVFile(t *testing.T) {
	dataframe, err := QueryFiles("select * from csv1", []string{"../testdata/csv1.csv"}, map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	data, err := dataframe.Data()
	assert.Equal(t, 4, len(data))
	schema, err := dataframe.Schema()
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, schema[0].Name, "c1")
	assert.Equal(t, schema[0].Format.Name(), "string")
	assert.Equal(t, schema[0].Format.Type(), reflect.String)

	// check aliasing
	dataframe, err = QueryFiles("select * from c", []string{"../testdata/csv1.csv#c"}, map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	data, err = dataframe.Data()
	assert.Equal(t, 4, len(data))
	schema, err = dataframe.Schema()
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, schema[0].Name, "c1")
	assert.Equal(t, schema[0].Format.Name(), "string")
	assert.Equal(t, schema[0].Format.Type(), reflect.String)

	// wrong file path
	dataframe, err = QueryFiles("select * from c", []string{"../testdata/csv1.csv1#c"}, map[string]string{})
	assert.Error(t, err)

}

func TestQuerySingleJSONFile(t *testing.T) {
	dataframe, err := QueryFiles("select * from json1", []string{"../testdata/json1.json"}, map[string]string{
		sources.ConfigJSONSingleLine: "false",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	data, err := dataframe.Data()
	assert.Equal(t, 4, len(data))
	schema, err := dataframe.Schema()
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, schema[0].Name, "c1")
	assert.Equal(t, schema[0].Format.Name(), "double")
	assert.Equal(t, schema[0].Format.Type(), reflect.Float64)
}

func TestQueryMultiFile(t *testing.T) {
	// check dirs
	dataframe, err := QueryFiles("select * from multiplefiles", []string{"../testdata/multiplefiles/"}, map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	data, err := dataframe.Data()
	assert.Equal(t, 12, len(data))
	schema, err := dataframe.Schema()
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, schema[0].Name, "c1")

	// check patterns
	dataframe, err = QueryFiles("select * from multifiles", []string{"../testdata/multiplefiles/*.csv#multifiles"}, map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	data, err = dataframe.Data()
	assert.Equal(t, 12, len(data))
	schema, err = dataframe.Schema()
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, schema[0].Name, "c1")
}

func TestQueryCompressedFile(t *testing.T) {
	// check gz
	dataframe, err := QueryFiles("select * from csv", []string{"../testdata/compressed/csv.csv.gz"}, map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	data, err := dataframe.Data()
	assert.Equal(t, 4, len(data))
	schema, err := dataframe.Schema()
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, schema[0].Name, "c1")

	//zip
	dataframe, err = QueryFiles("select * from csv", []string{"../testdata/compressed/csv.csv.zip"}, map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	data, err = dataframe.Data()
	assert.Equal(t, 12, len(data))
	schema, err = dataframe.Schema()
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, schema[0].Name, "c1")
}

func BenchmarkDataframeQuery(b *testing.B) {
	source, _ := sources.GetSource("json")
	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonStringData := jsonString
	for i := 1; i < 1000; i++ {
		jsonStringData = jsonStringData + "\n" + jsonString
	}

	jsonReader, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{})
	dataframe := sources.NewDatasourceDataFrame("t1", jsonReader)
	dataframe.Schema()

	for i := 0; i < b.N; i++ {
		queryDataFrames("select * from t1", []df.DataFrame{&dataframe}, map[string]string{})
	}

}

func BenchmarkMultipleDataframeQuery(b *testing.B) {
	source, _ := sources.GetSource("json")
	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonStringData := jsonString
	for i := 1; i < 1000; i++ {
		jsonStringData = jsonStringData + "\n" + jsonString
	}

	jsonReader, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{})
	dataframe := sources.NewDatasourceDataFrame("t1", jsonReader)
	jsonReader2, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{})
	dataframe2 := sources.NewDatasourceDataFrame("t2", jsonReader2)
	dataframe.Schema()

	for i := 0; i < b.N; i++ {
		queryDataFrames("select count(*) from t1,t2", []df.DataFrame{&dataframe, &dataframe2}, map[string]string{})
	}

}
