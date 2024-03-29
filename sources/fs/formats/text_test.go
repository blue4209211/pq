package formats

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTextDataSource(t *testing.T) {
	source := TextDataSource{}
	assert.Equal(t, source.Name(), "text")
}

func TestTextDataSourceReader(t *testing.T) {
	source := TextDataSource{}
	textString := `abcd
efgh
ijkl`

	textReader, err := source.Reader(strings.NewReader(textString), map[string]string{})
	assert.NoError(t, err)

	schema := textReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 2, schema.Len())
	assert.Equal(t, "text", schema.Get(0).Name)
	assert.Equal(t, "string", schema.Get(0).Format.Name())
	assert.Equal(t, "rowNumber_", schema.Get(1).Name)
	assert.Equal(t, "integer", schema.Get(1).Format.Name())
	data := *(textReader.Data())
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, "abcd", data[0].GetRaw(0))
	assert.Equal(t, int64(1), data[0].GetRaw(1))
	assert.Equal(t, "efgh", data[1].GetRaw(0))
	assert.Equal(t, int64(2), data[1].GetRaw(1))
	assert.Equal(t, "ijkl", data[2].GetRaw(0))
	assert.Equal(t, int64(3), data[2].GetRaw(1))

}

func BenchmarkTextParsing(b *testing.B) {
	source := TextDataSource{}
	textString := `abc def geh ijk lmn opq rst uvw xyz`

	textStringData := textString
	for i := 1; i < 1000; i++ {
		textStringData = textStringData + "\n" + textString
	}

	b.Run("perf-alldata", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			textReader, _ := source.Reader(strings.NewReader(textStringData), map[string]string{})
			textReader.Schema()
			textReader.Data()
		}
	})

}
