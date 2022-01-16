package sources

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

type csvDataSource struct {
}

func (t *csvDataSource) Args() map[string]string {
	return csvConfig
}

func (t *csvDataSource) Name() string {
	return "csv"
}

func (t *csvDataSource) Reader(reader io.Reader, args map[string]string) (DataFrameReader, error) {
	return &csvDataSourceReader{reader: reader, args: args}, nil
}

func (t *csvDataSource) Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error) {
	return &csvDataSourceWriter{data: data, args: args}, nil
}

type csvDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (t *csvDataSourceWriter) Write(writer io.Writer) (err error) {
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	seprator, err := getColSeprator(t.args)
	if err != nil {
		return
	}
	csvWriter.Comma = seprator
	dataArr, err := t.data.Data()
	if err != nil {
		return
	}

	isHeader, err := isHeaderEnabled(t.args)
	if err != nil {
		return
	}

	if isHeader {
		schema, err := t.data.Schema()
		if err != nil {
			return err
		}
		cols := make([]string, len(schema))

		for i, c := range schema {
			cols[i] = c.Name
		}
		csvWriter.Write(cols)
	}

	format, err := df.GetFormat("string")
	for _, rowInterface := range dataArr {
		row := make([]string, len(rowInterface))
		for i, r := range rowInterface {
			str, _ := format.Convert(r)
			row[i] = str.(string)
		}
		csvWriter.Write(row)
	}
	return
}

type csvDataSourceReader struct {
	reader   io.Reader
	args     map[string]string
	records  [][]string
	isHeader bool
}

func (t *csvDataSourceReader) Schema() (columns []df.Column, err error) {
	err = t.init()
	if err != nil {
		return
	}

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

func (t *csvDataSourceReader) Data() (data [][]interface{}, err error) {
	err = t.init()
	if err != nil {
		return
	}

	index := 0
	if t.isHeader {
		index = 1
	}

	data = make([][]interface{}, len(t.records)-index)

	for i, record := range t.records[index:] {
		row := make([]interface{}, len(record))
		for j, cell := range record {
			row[j] = cell
		}

		data[i] = row
	}
	return
}

func (t *csvDataSourceReader) init() (err error) {
	if t.records != nil {
		return nil
	}
	csvReader := csv.NewReader(t.reader)
	seprator, err := getColSeprator(t.args)
	if err != nil {
		return
	}
	csvReader.Comma = seprator
	records, err := csvReader.ReadAll()
	if err != nil {
		return
	}
	t.records = records

	isHeader, err := isHeaderEnabled(t.args)
	if err != nil {
		return
	}
	t.isHeader = isHeader

	return
}

func isHeaderEnabled(args map[string]string) (header bool, err error) {
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

func getColSeprator(args map[string]string) (sep rune, err error) {
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
