package formats

import (
	"bufio"
	"errors"
	"io"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
)

var textConfig = map[string]string{}

type TextDataSource struct {
}

func (t *TextDataSource) Args() map[string]string {
	return textConfig
}

func (t *TextDataSource) Name() string {
	return "text"
}

func (t *TextDataSource) Reader(reader io.Reader, args map[string]string) (FormatReader, error) {
	textReader := textDataSourceReader{args: args}
	err := textReader.init(reader)
	return &textReader, err
}

func (t *TextDataSource) Writer(data df.DataFrame, args map[string]string) (w FormatWriter, err error) {
	return w, errors.New("Unsupported")
}

type textDataSourceReader struct {
	args     map[string]string
	records  []df.Row
	isHeader bool
}

var textSchema df.DataFrameSchema = df.NewSchema([]df.SeriesSchema{
	{Name: "text", Format: df.StringFormat},
	{Name: "rowNumber_", Format: df.IntegerFormat},
})

func (t *textDataSourceReader) Schema() df.DataFrameSchema {
	return textSchema
}

func (t *textDataSourceReader) Data() *[]df.Row {
	return &t.records
}

func (t *textDataSourceReader) init(reader io.Reader) (err error) {
	bufferedReader := bufio.NewReader(reader)
	t.records = make([]df.Row, 0, 1000)
	schema := t.Schema()

	// in somecases line size gets bigger than default scanner settings
	// so using reader to handle those scenarios
	textData := make([]byte, 0, 10000)
	cnt := int64(1)
	for err == nil {
		textArr, isPrefix, err := bufferedReader.ReadLine()
		if isPrefix {
			textData = append(textData, textArr...)
			continue
		}
		if err == io.EOF {
			if len(textData) > 0 {
				rowData := []df.Value{
					inmemory.NewStringValueConst(string(textData) + string(textArr)), inmemory.NewIntValueConst(cnt),
				}
				t.records = append(t.records, inmemory.NewRow(&schema, &rowData))
				textData = textData[:0]
			} else if len(textArr) > 0 {
				rowData := []df.Value{
					inmemory.NewStringValueConst(string(textArr)), inmemory.NewIntValueConst(cnt),
				}
				t.records = append(t.records, inmemory.NewRow(&schema, &rowData))
			}
			break
		}

		if len(textData) > 0 {
			rowData := []df.Value{
				inmemory.NewStringValueConst(string(textData) + string(textArr)), inmemory.NewIntValueConst(cnt),
			}
			t.records = append(t.records, inmemory.NewRow(&schema, &rowData))
			textData = textData[:0]
		} else {
			rowData := []df.Value{
				inmemory.NewStringValueConst(string(textArr)), inmemory.NewIntValueConst(cnt),
			}
			t.records = append(t.records, inmemory.NewRow(&schema, &rowData))
		}
		cnt = cnt + 1
	}
	return nil
}
