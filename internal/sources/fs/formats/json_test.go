package formats

import (
	"strings"
	"testing"

	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestJSONDataSource(t *testing.T) {
	source := JsonDataSource{}
	assert.Equal(t, source.Name(), "json")
}

func TestCustomJSONParser(t *testing.T) {
	jsonString := `[{"a":1, "b":2.0, "c":"c11\"234", "d":false, "e":[1,2,3], "f":{"k":1}, "g":null}]`
	jsonBytes := []byte(jsonString)
	objMapList, err := jsonReadToArray(&jsonBytes, true, "")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(objMapList))
	assert.Equal(t, float64(1), objMapList[0]["a"])
	assert.Equal(t, float64(2.0), objMapList[0]["b"])
	assert.Equal(t, `c11"234`, objMapList[0]["c"])
	assert.Equal(t, false, objMapList[0]["d"])
	assert.Equal(t, "[1,2,3]", objMapList[0]["e"])
	assert.Equal(t, `{"k":1}`, objMapList[0]["f"])
	assert.Equal(t, nil, objMapList[0]["g"])
}

func TestJSONDataSourceReader(t *testing.T) {
	source := JsonDataSource{}

	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonReader, err := source.Reader(strings.NewReader(jsonString), map[string]string{})
	assert.NoError(t, err)

	schema := jsonReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, schema.Len())
	assert.Equal(t, "a", schema.Get(0).Name)
	assert.Equal(t, "double", schema.Get(0).Format.Name())
	assert.Equal(t, "b", schema.Get(1).Name)
	assert.Equal(t, "double", schema.Get(1).Format.Name())
	assert.Equal(t, "c", schema.Get(2).Name)
	assert.Equal(t, "string", schema.Get(2).Format.Name())
	assert.Equal(t, "d", schema.Get(3).Name)
	assert.Equal(t, "string", schema.Get(3).Format.Name())
	data := *(jsonReader.Data())
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, 1.0, data[0].GetRaw(0))
	assert.Equal(t, 2.0, data[0].GetRaw(1))
	assert.Equal(t, "c1", data[0].GetRaw(2))
	assert.Equal(t, "d1", data[0].GetRaw(3))
	assert.Equal(t, nil, data[2].GetRaw(1))
	assert.Equal(t, "", data[2].GetRaw(2))

	//multiline json
	multiLineJSONString := `[
	{"a":1, "b":2, "c":"c1", "d":"d1"},
	{"a":3, "b":4, "c":"c2", "d":"d,2"},
	{"a":5, "b":null, "c":"", "d":"d2"}
]
`

	jsonReader, err = source.Reader(strings.NewReader(multiLineJSONString), map[string]string{
		ConfigJSONSingleLine: "false",
	})
	assert.NoError(t, err)

	schema = jsonReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, schema.Len())
	assert.Equal(t, "a", schema.Get(0).Name)
	assert.Equal(t, "double", schema.Get(0).Format.Name())
	assert.Equal(t, "b", schema.Get(1).Name)
	assert.Equal(t, "double", schema.Get(1).Format.Name())
	assert.Equal(t, "c", schema.Get(2).Name)
	assert.Equal(t, "string", schema.Get(3).Format.Name())
	assert.Equal(t, "d", schema.Get(3).Name)
	assert.Equal(t, "string", schema.Get(3).Format.Name())
	data = *(jsonReader.Data())
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, 1.0, data[0].GetRaw(0))
	assert.Equal(t, 2.0, data[0].GetRaw(1))
	assert.Equal(t, "c1", data[0].GetRaw(2))
	assert.Equal(t, "d1", data[0].GetRaw(3))
	assert.Equal(t, nil, data[2].GetRaw(1))
	assert.Equal(t, "", data[2].GetRaw(2))
}

func TestJSONDataSourceWriter(t *testing.T) {
	source := JsonDataSource{}

	jsonString := `[{"a":1,"b":2,"c":"c1","d":"d1"},{"a":3,"b":4,"c":"c2","d":"d,2"},{"a":5,"b":null,"c":"","d":"d2"}]
`

	jsonReader, err := source.Reader(strings.NewReader(jsonString), map[string]string{})
	assert.NoError(t, err)

	dataframe := inmemory.NewDataframeFromRowAndName("df_1", jsonReader.Schema(), jsonReader.Data())
	assert.Equal(t, dataframe.Name(), "df_1")

	writer, err := source.Writer(dataframe, map[string]string{
		ConfigJSONSingleLine: "false",
	})
	assert.Nil(t, err)
	buff := new(strings.Builder)
	writer.Write(buff)
	assert.Equal(t, jsonString, buff.String())
}

func BenchmarkJSONParsing(b *testing.B) {
	source := JsonDataSource{}
	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`
	jsonAllString := `{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}`

	jsonStringData := jsonString
	for i := 1; i < 1000; i++ {
		jsonStringData = jsonStringData + "\n" + jsonString
	}

	jsonAllStringData := jsonAllString
	for i := 1; i < 1000; i++ {
		jsonAllStringData = jsonAllString + "," + jsonAllString
	}
	jsonAllStringData = "[" + jsonAllStringData + "]"

	b.Run("perf-alldata", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jsonReader, _ := source.Reader(strings.NewReader(jsonAllStringData), map[string]string{
				ConfigJSONSingleLine: "false",
			})
			jsonReader.Schema()
			jsonReader.Data()
		}
	})

	b.Run("perf-singleline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jsonReader, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{
				ConfigJSONSingleLine: "true",
			})
			jsonReader.Schema()
			jsonReader.Data()
		}
	})

}
