package files

import (
	"strings"
	"testing"

	"github.com/blue4209211/pq/internal/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestJSONDataSource(t *testing.T) {
	source := jsonDataSource{}
	assert.Equal(t, source.Name(), "json")
}

func TestCustomJSONParser(t *testing.T) {
	jsonString := `[{"a":1, "b":2.0, "c":"c11\"234", "d":false, "e":[1,2,3], "f":{"k":1}, "g":null}]`
	objMapList, err := jsonReadToArray([]byte(jsonString), true, "")
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
	source := jsonDataSource{}

	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonReader, err := source.Reader(strings.NewReader(jsonString), map[string]string{})
	assert.NoError(t, err)

	schema := jsonReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, len(schema))
	assert.Equal(t, "a", schema[0].Name)
	assert.Equal(t, "double", schema[0].Format.Name())
	assert.Equal(t, "b", schema[1].Name)
	assert.Equal(t, "double", schema[1].Format.Name())
	assert.Equal(t, "c", schema[2].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	assert.Equal(t, "d", schema[3].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	data := jsonReader.Data()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, 1.0, data[0][0])
	assert.Equal(t, 2.0, data[0][1])
	assert.Equal(t, "c1", data[0][2])
	assert.Equal(t, "d1", data[0][3])
	assert.Equal(t, nil, data[2][1])
	assert.Equal(t, "", data[2][2])

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
	assert.Equal(t, 4, len(schema))
	assert.Equal(t, "a", schema[0].Name)
	assert.Equal(t, "double", schema[0].Format.Name())
	assert.Equal(t, "b", schema[1].Name)
	assert.Equal(t, "double", schema[1].Format.Name())
	assert.Equal(t, "c", schema[2].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	assert.Equal(t, "d", schema[3].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	data = jsonReader.Data()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, 1.0, data[0][0])
	assert.Equal(t, 2.0, data[0][1])
	assert.Equal(t, "c1", data[0][2])
	assert.Equal(t, "d1", data[0][3])
	assert.Equal(t, nil, data[2][1])
	assert.Equal(t, "", data[2][2])
}

func TestJSONDataSourceWriter(t *testing.T) {
	source := jsonDataSource{}

	jsonString := `[{"a":1,"b":2,"c":"c1","d":"d1"},{"a":3,"b":4,"c":"c2","d":"d,2"},{"a":5,"b":null,"c":"","d":"d2"}]
`

	jsonReader, err := source.Reader(strings.NewReader(jsonString), map[string]string{})
	assert.NoError(t, err)

	dataframe := inmemory.NewDataframeWithName("df_1", jsonReader.Schema(), jsonReader.Data())
	assert.Equal(t, dataframe.Name(), "df_1")

	writer, err := source.Writer(dataframe, map[string]string{
		ConfigJSONSingleLine: "false",
	})
	buff := new(strings.Builder)
	writer.Write(buff)
	assert.Equal(t, jsonString, buff.String())
}

func BenchmarkJSONParsing(b *testing.B) {
	source := jsonDataSource{}
	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonStringData := jsonString
	for i := 1; i < 1000; i++ {
		jsonStringData = jsonStringData + "\n" + jsonString
	}

	b.Run("perf-alldata", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jsonReader, _ := source.Reader(strings.NewReader(jsonStringData), map[string]string{
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
