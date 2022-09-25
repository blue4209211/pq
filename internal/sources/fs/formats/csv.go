package formats

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/blue4209211/pq/df"
)

// ConfigCsvHeader Is First valid line header
const ConfigCsvHeader = "csv.hasHeader"

// ConfigCsvSep File Seprator, Default = ,
const ConfigCsvSep = "csv.sep"

var csvConfig = map[string]string{
	ConfigCsvHeader: "true",
	ConfigCsvSep:    ",",
}

type CsvDataSource struct {
}

func (t *CsvDataSource) Args() map[string]string {
	return csvConfig
}

func (t *CsvDataSource) Name() string {
	return "csv"
}

func (t *CsvDataSource) Reader(reader io.Reader, args map[string]string) (FormatReader, error) {
	csvReader := csvDataSourceReader{args: args}
	err := csvReader.init(reader)
	return &csvReader, err
}

func (t *CsvDataSource) Writer(data df.DataFrame, args map[string]string) (FormatWriter, error) {
	return &csvDataSourceWriter{data: data, args: args}, nil
}

type csvDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (t *csvDataSourceWriter) Write(writer io.Writer) (err error) {
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	seprator, err := csvGetColSeprator(t.args)
	if err != nil {
		return
	}
	csvWriter.Comma = seprator
	isHeader, err := csvIsHeaderEnabled(t.args)
	if err != nil {
		return
	}

	if isHeader {
		schema := t.data.Schema()
		cols := make([]string, schema.Len())

		for i, c := range schema.Columns() {
			cols[i] = c.Name
		}
		csvWriter.Write(cols)
	}

	format, err := df.GetFormat("string")
	for i := int64(0); i < t.data.Len(); i++ {
		rowInterface := t.data.Get(i)
		row := make([]string, rowInterface.Len())
		for i, r := range rowInterface.Data() {
			str, _ := format.Convert(r)
			row[i] = str.(string)
		}
		csvWriter.Write(row)
	}
	return
}

type csvDataSourceReader struct {
	args     map[string]string
	records  [][]string
	isHeader bool
}

func (t *csvDataSourceReader) Schema() (columns []df.Column) {
	columns = make([]df.Column, len(t.records[0]))
	f, _ := df.GetFormat("string")
	for i, col := range t.records[0] {
		if t.isHeader {
			columns[i] = df.Column{Name: col, Format: f}
		} else {
			columns[i] = df.Column{Name: "c" + strconv.Itoa(i), Format: f}
		}
	}
	return
}

func (t *csvDataSourceReader) Data() (data [][]any) {
	index := 0
	if t.isHeader {
		index = 1
	}

	data = make([][]any, len(t.records)-index)

	for i, record := range t.records[index:] {
		row := make([]any, len(record))
		for j, cell := range record {
			row[j] = cell
		}

		data[i] = row
	}
	return
}

func (t *csvDataSourceReader) init(reader io.Reader) (err error) {
	csvReader := csv.NewReader(reader)
	seprator, err := csvGetColSeprator(t.args)
	if err != nil {
		return
	}
	csvReader.Comma = seprator
	records, err := csvReader.ReadAll()
	if err != nil {
		return
	}
	t.records = records

	isHeader, err := csvIsHeaderEnabled(t.args)
	if err != nil {
		return
	}
	t.isHeader = isHeader

	return
}

func csvIsHeaderEnabled(args map[string]string) (header bool, err error) {
	headerStr, ok := args[ConfigCsvHeader]
	header = true
	if ok {
		header, err = strconv.ParseBool(headerStr)
		if err != nil {
			return
		}
	}

	return
}

func csvGetColSeprator(args map[string]string) (sep rune, err error) {
	seprator, ok := args[ConfigCsvSep]
	if !ok {
		seprator = ","
	}

	if len(seprator) == 1 {
		r, _ := utf8.DecodeRuneInString(seprator)
		sep = r
	} else if seprator == `\t` {
		sep = '\t'
	} else {
		err = errors.New("Unsupported seprator - " + seprator)
	}

	return
}
