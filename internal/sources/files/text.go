package files

import (
	"bufio"
	"errors"
	"io"

	"github.com/blue4209211/pq/df"
)

var textConfig = map[string]string{}

type textDataSource struct {
}

func (t *textDataSource) Args() map[string]string {
	return textConfig
}

func (t *textDataSource) Name() string {
	return "text"
}

func (t *textDataSource) Reader(reader io.Reader, args map[string]string) (StreamReader, error) {
	textReader := textDataSourceReader{args: args}
	err := textReader.init(reader)
	return &textReader, err
}

func (t *textDataSource) Writer(data df.DataFrame, args map[string]string) (w StreamWriter, err error) {
	return w, errors.New("Unsupported")
}

type textDataSourceReader struct {
	args     map[string]string
	records  [][]any
	isHeader bool
}

var textSchema []df.Column = []df.Column{
	{Name: "text", Format: df.StringFormat},
	{Name: "rowNumber_", Format: df.IntegerFormat},
}

func (t *textDataSourceReader) Schema() (columns []df.Column) {
	return textSchema
}

func (t *textDataSourceReader) Data() (data [][]any) {
	return t.records
}

func (t *textDataSourceReader) init(reader io.Reader) (err error) {
	bufferedReader := bufio.NewReader(reader)
	t.records = make([][]any, 0, 1000)

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
				rowData := []any{
					string(textData) + string(textArr), cnt,
				}
				t.records = append(t.records, rowData)
				textData = textData[:0]
			} else if len(textArr) > 0 {
				rowData := []any{
					string(textArr), cnt,
				}
				t.records = append(t.records, rowData)
			}
			break
		}

		if len(textData) > 0 {
			rowData := []any{
				string(textData) + string(textArr), cnt,
			}
			t.records = append(t.records, rowData)
			textData = textData[:0]
		} else {
			rowData := []any{
				string(textArr), cnt,
			}
			t.records = append(t.records, rowData)
		}
		cnt = cnt + 1
	}
	return nil
}
