package sources

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONDataSource(t *testing.T) {
	source := JSONDataSource{}
	assert.Equal(t, source.Name(), "json")
}

func TestJSONDataSourceReader(t *testing.T) {
	source := JSONDataSource{}

	jsonString := `[{"a":1, "b":2, "c":"c1", "d":"d1"},{"a":3, "b":4, "c":"c2", "d":"d,2"},{"a":5, "b":null, "c":"", "d":"d2"}]`

	jsonReader, err := source.Reader(strings.NewReader(jsonString), map[string]string{})
	assert.NoError(t, err)

	schema, err := jsonReader.Schema()

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
	data, err := jsonReader.Data()
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
	source := JSONDataSource{}

	jsonString := `[{"a":1,"b":2,"c":"c1","d":"d1"},{"a":3,"b":4,"c":"c2","d":"d,2"},{"a":5,"b":null,"c":"","d":"d2"}]
`

	jsonReader, err := source.Reader(strings.NewReader(jsonString), map[string]string{})
	assert.NoError(t, err)

	dataframe := NewDatasourceDataFrame("df_1", jsonReader)
	writer, err := source.Writer(&dataframe, map[string]string{})
	buff := new(strings.Builder)
	writer.Write(buff)
	assert.Equal(t, jsonString, buff.String())

}
