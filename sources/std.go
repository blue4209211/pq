package sources

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/blue4209211/pq/df"
)

// ConfigStdType source format for StdIn/Out
const ConfigStdType = "std.type"

var stdConfig = map[string]string{
	ConfigStdType: "json",
}

type stdDataSource struct {
}

func (t *stdDataSource) Name() string {
	return "std"
}

func (t *stdDataSource) Args() map[string]string {
	return stdConfig
}

func (t *stdDataSource) Reader(reader io.Reader, args map[string]string) (DataFrameReader, error) {
	return &stdDataSourceReader{reader: reader, args: args}, nil
}

func (t *stdDataSource) Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error) {
	return &stdDataSourceWriter{data: data, args: args}, nil
}

type stdDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (t *stdDataSourceWriter) Write(writer io.Writer) error {
	return errors.New("Unsupported")
}

type stdDataSourceReader struct {
	reader  io.Reader
	args    map[string]string
	records [][]string
}

func (t *stdDataSourceReader) Schema() (columns []df.Column, err error) {
	err = t.init()
	if err != nil {
		return
	}

	headerStr, ok := t.args["fmt.csv.header"]
	header := true
	if ok {
		header, err = strconv.ParseBool(headerStr)
		if err != nil {
			return
		}
	}

	columns = make([]df.Column, len(t.records[0]))
	f, _ := df.GetFormat("string")
	for i, col := range t.records[0] {
		if header {
			columns[i] = df.Column{Name: col, Format: f}
		} else {
			columns[i] = df.Column{Name: "col_" + strconv.Itoa(i), Format: f}
		}
	}
	return
}

func (t *stdDataSourceReader) Data() (data [][]interface{}, err error) {
	err = t.init()
	if err != nil {
		return
	}

	data = make([][]interface{}, len(t.records)-1)
	for i, record := range t.records[1:] {
		row := make([]interface{}, len(record))
		for j, cell := range record {
			row[j] = cell
		}

		data[i] = row
	}
	return
}

func (t *stdDataSourceReader) init() (err error) {
	if t.records != nil {
		return nil
	}
	csvReader := csv.NewReader(t.reader)
	seprator, ok := t.args["fmt.csv.sep"]
	if ok {
		if len(seprator) == 1 {
			r, _ := utf8.DecodeRuneInString(seprator)
			csvReader.Comma = r
		} else if seprator == `\t` {
			csvReader.Comma = '\t'
		} else {
			return errors.New("Unsupported seprator - " + seprator)
		}
	}
	records, err := csvReader.ReadAll()
	if err != nil {
		return
	}
	t.records = records
	return
}
