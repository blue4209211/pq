package engine

import (
	"reflect"
	"strings"
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
	"github.com/blue4209211/pq/internal/sources"
	"github.com/blue4209211/pq/internal/sources/fs/formats"
	"github.com/stretchr/testify/assert"
)

func queryFiles(query string, fileOrDrs []string, config map[string]string) (data df.DataFrame, err error) {
	dfs, err := sources.ReadDataFrames(config, fileOrDrs...)
	if err != nil {
		return data, err
	}
	return QueryDataFrames(query, dfs, config)
}

func TestQuerySingleCSVFile(t *testing.T) {
	dataframe, err := queryFiles("select * from csv1", []string{"../../testdata/csv1.csv"}, map[string]string{
		ConfigEngineStorage: "memory",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(4), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")
	assert.Equal(t, schema.Get(0).Format.Name(), "string")
	assert.Equal(t, schema.Get(0).Format.Type(), reflect.String)

	// check aliasing
	dataframe, err = queryFiles("select * from c", []string{"../../testdata/csv1.csv#c"}, map[string]string{
		ConfigEngineStorage: "memory",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(4), dataframe.Len())
	schema = dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")
	assert.Equal(t, schema.Get(0).Format.Name(), "string")
	assert.Equal(t, schema.Get(0).Format.Type(), reflect.String)

	// wrong file path
	dataframe, err = queryFiles("select * from c", []string{"../../testdata/csv1.csv1#c"}, map[string]string{
		ConfigEngineStorage: "memory",
	})
	assert.Error(t, err)

}

func TestQuerySingleJSONFile(t *testing.T) {
	dataframe, err := queryFiles("select * from json1", []string{"../../testdata/json1.json"}, map[string]string{
		formats.ConfigJSONSingleLine: "false",
		ConfigEngineStorage:          "memory",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(4), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")
	assert.Equal(t, schema.Get(0).Format.Name(), "double")
	assert.Equal(t, schema.Get(0).Format.Type(), reflect.Float64)
}

func TestQuerySingleXMLFile(t *testing.T) {
	dataframe, err := queryFiles("select * from xml1", []string{"../../testdata/xml1.xml"}, map[string]string{
		formats.ConfigXMLSingleLine:  "false",
		formats.ConfigXMLElementName: "element",
		ConfigEngineStorage:          "memory",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(3), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 4, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "_a")
	assert.Equal(t, schema.Get(0).Format.Name(), "string")
	assert.Equal(t, schema.Get(0).Format.Type(), reflect.String)
}

func TestQuerySingleParquetFile(t *testing.T) {
	dataframe, err := queryFiles("select * from parquet1", []string{"../../testdata/parquet1.parquet"}, map[string]string{
		formats.ConfigParquetSingleLine: "false",
		ConfigEngineStorage:             "memory",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(3), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 4, schema.Len())
	assert.Equal(t, "a", schema.Get(0).Name)
	assert.Equal(t, "integer", schema.Get(0).Format.Name())
	assert.Equal(t, reflect.Int64, schema.Get(0).Format.Type())
}

func TestQueryMultiFile(t *testing.T) {
	// check dirs
	dataframe, err := queryFiles("select * from multiplefiles", []string{"../../testdata/multiplefiles/"}, map[string]string{
		ConfigEngineStorage: "memory",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(12), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")

	// check patterns
	dataframe, err = queryFiles("select * from multifiles", []string{"../../testdata/multiplefiles/*.csv#multifiles"}, map[string]string{
		ConfigEngineStorage: "memory",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(12), dataframe.Len())
	schema = dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")
}

func TestQueryCompressedFile(t *testing.T) {
	// check gz
	dataframe, err := queryFiles("select * from csv", []string{"../../testdata/compressed/csv.csv.gz"}, map[string]string{
		ConfigEngineStorage: "memory",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(4), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")

	//zip
	dataframe, err = queryFiles("select * from csv", []string{"../../testdata/compressed/csv.csv.zip"}, map[string]string{
		ConfigEngineStorage: "memory",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(12), dataframe.Len())
	schema = dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")
}

func BenchmarkDataframeQuery(b *testing.B) {
	source, _ := formats.GetFormatHandler("json")
	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonStringData := jsonString
	for i := 1; i < 1000; i++ {
		jsonStringData = jsonStringData + "\n" + jsonString
	}

	jsonReader, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{})
	dataframe := inmemory.NewDataframeWithName("t1", jsonReader.Schema(), jsonReader.Data())
	dataframe.Schema()

	for i := 0; i < b.N; i++ {
		QueryDataFrames("select * from t1", []df.DataFrame{dataframe}, map[string]string{
			ConfigEngineStorage: "memory",
		})
	}

}

func BenchmarkMultipleDataframeQuery(b *testing.B) {
	source, _ := formats.GetFormatHandler("json")
	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonStringData := jsonString
	for i := 1; i < 1000; i++ {
		jsonStringData = jsonStringData + "\n" + jsonString
	}

	jsonReader, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{})
	dataframe := inmemory.NewDataframeWithName("t1", jsonReader.Schema(), jsonReader.Data())
	jsonReader2, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{})
	dataframe2 := inmemory.NewDataframeWithName("t2", jsonReader2.Schema(), jsonReader2.Data())
	dataframe.Schema()

	for i := 0; i < b.N; i++ {
		QueryDataFrames("select count(*) from t1,t2", []df.DataFrame{dataframe, dataframe2}, map[string]string{
			ConfigEngineStorage: "memory",
		})
	}

}
