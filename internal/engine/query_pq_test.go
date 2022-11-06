package engine

import (
	"reflect"
	"strings"
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/blue4209211/pq/sources/fs/formats"
	"github.com/stretchr/testify/assert"
)

func TestQuerySortOpPQ(t *testing.T) {
	dataframe, err := queryFiles("select * from json1 order by c1 desc", []string{"../../testdata/json1.json"}, map[string]string{
		ConfigEngineStorage:          "pq",
		formats.ConfigJSONSingleLine: "false",
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), dataframe.Len())
	assert.Equal(t, float64(4), dataframe.GetSeriesByName("c1").Get(0).Get())
}

func TestQueryFilterOpPQ(t *testing.T) {
	dataframe, err := queryFiles("select * from json1 where c1 > 2.0 and c2 = 'c'", []string{"../../testdata/json1.json"}, map[string]string{
		ConfigEngineStorage:          "pq",
		formats.ConfigJSONSingleLine: "false",
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), dataframe.Len())

	dataframe, err = queryFiles("select * from json1 where c3 match 'a' ", []string{"../../testdata/json1.json"}, map[string]string{
		ConfigEngineStorage:          "pq",
		formats.ConfigJSONSingleLine: "false",
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), dataframe.Len())

	dataframe, err = queryFiles("select * from json1 where c3 like '%1' ", []string{"../../testdata/json1.json"}, map[string]string{
		ConfigEngineStorage:          "pq",
		formats.ConfigJSONSingleLine: "false",
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(2), dataframe.Len())

	dataframe, err = queryFiles("select * from json1 where c3 regexp '.*1' ", []string{"../../testdata/json1.json"}, map[string]string{
		ConfigEngineStorage:          "pq",
		formats.ConfigJSONSingleLine: "false",
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(2), dataframe.Len())

}

func TestQuerySingleCSVFilePQ(t *testing.T) {
	dataframe, err := queryFiles("select * from csv1", []string{"../../testdata/csv1.csv"}, map[string]string{
		ConfigEngineStorage: "pq",
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
		ConfigEngineStorage: "pq",
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
		ConfigEngineStorage: "pq",
	})
	assert.Error(t, err)

}

func TestQuerySingleJSONFilePQ(t *testing.T) {
	dataframe, err := queryFiles("select * from json1", []string{"../../testdata/json1.json"}, map[string]string{
		formats.ConfigJSONSingleLine: "false",
		ConfigEngineStorage:          "pq",
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

func TestQueryMultiFilePQ(t *testing.T) {
	// check dirs
	dataframe, err := queryFiles("select * from multiplefiles", []string{"../../testdata/multiplefiles/"}, map[string]string{
		ConfigEngineStorage: "pq",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(12), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")
}

func TestQueryRegexFilePQ(t *testing.T) {
	// check patterns
	dataframe, err := queryFiles("select * from multifiles", []string{"../../testdata/multiplefiles/*.csv#multifiles"}, map[string]string{
		ConfigEngineStorage: "pq",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(12), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")
}

func TestQueryCompressedFileGzPQ(t *testing.T) {
	// check gz
	dataframe, err := queryFiles("select * from csv", []string{"../../testdata/compressed/csv.csv.gz"}, map[string]string{
		ConfigEngineStorage: "pq",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(4), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")
}

func TestQueryCompressedFileZipPQ(t *testing.T) {
	//zip
	dataframe, err := queryFiles("select * from csv", []string{"../../testdata/compressed/csv.csv.zip"}, map[string]string{
		ConfigEngineStorage: "pq",
	})
	assert.NoError(t, err)
	assert.NotNil(t, dataframe)
	assert.Equal(t, int64(12), dataframe.Len())
	schema := dataframe.Schema()
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, schema.Get(0).Name, "c1")
}

func BenchmarkDataframeQueryPQ(b *testing.B) {
	source, _ := formats.GetFormatHandler("json")
	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonStringData := jsonString
	for i := 1; i < 1000; i++ {
		jsonStringData = jsonStringData + "\n" + jsonString
	}

	jsonReader, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{
		ConfigEngineStorage: "pq",
	})
	dataframe := inmemory.NewDataframeFromRowAndName("t1", jsonReader.Schema(), jsonReader.Data())
	dataframe.Schema()

	for i := 0; i < b.N; i++ {
		QueryDataFrames("select * from t1", []df.DataFrame{dataframe}, map[string]string{
			ConfigEngineStorage: "pq",
		})
	}

}

func BenchmarkMultipleDataframeQueryPQ(b *testing.B) {
	source, _ := formats.GetFormatHandler("json")
	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonStringData := jsonString
	for i := 1; i < 1000; i++ {
		jsonStringData = jsonStringData + "\n" + jsonString
	}

	jsonReader, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{})
	dataframe := inmemory.NewDataframeFromRowAndName("t1", jsonReader.Schema(), jsonReader.Data())
	jsonReader2, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{})
	dataframe2 := inmemory.NewDataframeFromRowAndName("t2", jsonReader2.Schema(), jsonReader2.Data())
	dataframe.Schema()

	for i := 0; i < b.N; i++ {
		QueryDataFrames("select count(*) from t1,t2", []df.DataFrame{dataframe, dataframe2}, map[string]string{
			ConfigEngineStorage: "pq",
		})
	}

}
