package formats

import (
	"strings"
	"testing"

	"github.com/blue4209211/pq/df/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestXMLDataSource(t *testing.T) {
	source := XmlDataSource{}
	assert.Equal(t, source.Name(), "xml")
}

func TestXMLDataSourceReader(t *testing.T) {
	source := XmlDataSource{}
	xmlString := `<root><element a="1"><b>2</b><c>c1</c><d>d1</d></element><element a="3"><b>4</b><c>c2</c><d>d,2</d></element><element a="5"><b></b><c></c><d>d2</d></element></root>`

	xmlReader, err := source.Reader(strings.NewReader(xmlString), map[string]string{
		ConfigXMLElementName: "element",
	})
	assert.NoError(t, err)

	schema := xmlReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, len(schema))
	assert.Equal(t, "_a", schema[0].Name)
	assert.Equal(t, "string", schema[0].Format.Name())
	assert.Equal(t, "b", schema[1].Name)
	assert.Equal(t, "string", schema[1].Format.Name())
	assert.Equal(t, "c", schema[2].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	assert.Equal(t, "d", schema[3].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	data := xmlReader.Data()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, "1", data[0][0])
	assert.Equal(t, "2", data[0][1])
	assert.Equal(t, "c1", data[0][2])
	assert.Equal(t, "d1", data[0][3])
	assert.Equal(t, "", data[2][1])
	assert.Equal(t, "", data[2][2])

	//multiline xml
	multiLineXMLString := `<root>
		<element a="1">
			<b>2</b>
			<c>c1</c>
			<d>d1</d>
		</element>
		<element a="3">
			<b>4</b>
			<c>c2</c>
			<d>d,2</d>
		</element>
		<element a="5">
			<b></b>
			<c></c>
			<d>d2</d>
		</element>
	</root>
	`

	xmlReader, err = source.Reader(strings.NewReader(multiLineXMLString), map[string]string{
		ConfigXMLSingleLine:  "false",
		ConfigXMLElementName: "element",
	})
	assert.NoError(t, err)

	schema = xmlReader.Schema()

	assert.NoError(t, err)
	assert.Equal(t, 4, len(schema))
	assert.Equal(t, "_a", schema[0].Name)
	assert.Equal(t, "string", schema[0].Format.Name())
	assert.Equal(t, "b", schema[1].Name)
	assert.Equal(t, "string", schema[1].Format.Name())
	assert.Equal(t, "c", schema[2].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	assert.Equal(t, "d", schema[3].Name)
	assert.Equal(t, "string", schema[3].Format.Name())
	data = xmlReader.Data()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(data))
	assert.Equal(t, "1", data[0][0])
	assert.Equal(t, "2", data[0][1])
	assert.Equal(t, "c1", data[0][2])
	assert.Equal(t, "d1", data[0][3])
	assert.Equal(t, "", data[2][1])
	assert.Equal(t, "", data[2][2])
}

func TestXMLDataSourceWriter(t *testing.T) {
	source := XmlDataSource{}

	xmlString := `<root><element a="1"><b>2</b><c>c1</c><d>d1</d></element><element a="3"><b>4</b><c>c2</c><d>d,2</d></element><element a="5"><b></b><c></c><d>d2</d></element></root>
`

	xmlReader, err := source.Reader(strings.NewReader(xmlString), map[string]string{
		ConfigXMLElementName: "element",
	})
	assert.NoError(t, err)

	dataframe := inmemory.NewDataframeWithName("df_1", xmlReader.Schema(), xmlReader.Data())
	assert.Equal(t, dataframe.Name(), "df_1")

	writer, err := source.Writer(dataframe, map[string]string{
		ConfigXMLSingleLine:  "false",
		ConfigXMLElementName: "element",
	})
	buff := new(strings.Builder)
	writer.Write(buff)
	assert.Equal(t, xmlString, buff.String())

	t.Log()
}

func BenchmarkXMLParsing(b *testing.B) {
	source := XmlDataSource{}
	xmlString := `<root><element a="1"><b>2</b><c>c1</c><d>d1</d></element><element a="3"><b>4</b><c>c2</c><d>d,2</d></element><element a="5"><b></b><c></c><d>d2</d></element></root>`

	xmlStringData := xmlString
	for i := 1; i < 1000; i++ {
		xmlStringData = xmlStringData + "\n" + xmlString
	}

	b.Run("perf-alldata", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			xmlReader, _ := source.Reader(strings.NewReader(xmlStringData), map[string]string{
				ConfigXMLElementName: "element",
				ConfigXMLSingleLine:  "false",
			})
			xmlReader.Schema()
			xmlReader.Data()
		}
	})

}
