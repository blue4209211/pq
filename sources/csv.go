package sources

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/blue4209211/pq/df"
)

const ConfigCsvHeader = "csv.header"
const ConfigCsvSep = "csv.sep"

var csvConfig = map[string]string{
	ConfigCsvHeader: "true",
	ConfigCsvSep:    ",",
}

type CSVDataSource struct {
}

func (self *CSVDataSource) Args() map[string]string {
	return csvConfig
}

func (self *CSVDataSource) Name() string {
	return "csv"
}

func (self *CSVDataSource) Reader(reader io.Reader, args map[string]string) (DataFrameReader, error) {
	return &CSVDataSourceReader{reader: reader, args: args}, nil
}

func (self *CSVDataSource) Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error) {
	return &CSVDataSourceWriter{data: data, args: args}, nil
}

type CSVDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (self *CSVDataSourceWriter) Write(writer io.Writer) (err error) {
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	seprator, err := getColSeprator(self.args)
	if err != nil {
		return
	}
	csvWriter.Comma = seprator
	dataArr, err := self.data.Data()
	if err != nil {
		return
	}

	isHeader, err := isHeaderEnabled(self.args)
	if err != nil {
		return
	}

	if isHeader {
		schema, err := self.data.Schema()
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

type CSVDataSourceReader struct {
	reader   io.Reader
	args     map[string]string
	records  [][]string
	isHeader bool
}

func (self *CSVDataSourceReader) Schema() (columns []df.Column, err error) {
	err = self.init()
	if err != nil {
		return
	}

	columns = make([]df.Column, len(self.records[0]))
	f, _ := df.GetFormat("string")
	for i, col := range self.records[0] {
		if self.isHeader {
			columns[i] = df.Column{Name: col, Format: f}
		} else {
			columns[i] = df.Column{Name: "c" + strconv.Itoa(i), Format: f}
		}
	}
	return
}

func (self *CSVDataSourceReader) Data() (data [][]interface{}, err error) {
	err = self.init()
	if err != nil {
		return
	}

	index := 0
	if self.isHeader {
		index = 1
	}

	data = make([][]interface{}, len(self.records)-index)

	for i, record := range self.records[index:] {
		row := make([]interface{}, len(record))
		for j, cell := range record {
			row[j] = cell
		}

		data[i] = row
	}
	return
}

func (self *CSVDataSourceReader) init() (err error) {
	if self.records != nil {
		return nil
	}
	csvReader := csv.NewReader(self.reader)
	seprator, err := getColSeprator(self.args)
	if err != nil {
		return
	}
	csvReader.Comma = seprator
	records, err := csvReader.ReadAll()
	if err != nil {
		return
	}
	self.records = records

	isHeader, err := isHeaderEnabled(self.args)
	if err != nil {
		return
	}
	self.isHeader = isHeader

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
