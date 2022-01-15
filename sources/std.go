package sources

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/blue4209211/pq/df"
)

const ConfigStdType = "std.type"

var stdConfig = map[string]string{
	ConfigStdType: "json",
}

type StdDataSource struct {
}

func (self *StdDataSource) Name() string {
	return "std"
}

func (self *StdDataSource) Args() map[string]string {
	return stdConfig
}

func (self *StdDataSource) Reader(reader io.Reader, args map[string]string) (DataFrameReader, error) {
	return &StdDataSourceReader{reader: reader, args: args}, nil
}

func (self *StdDataSource) Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error) {
	return &StdDataSourceWriter{data: data, args: args}, nil
}

type StdDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (self *StdDataSourceWriter) Write(writer io.Writer) error {
	return errors.New("Unsupported")
}

type StdDataSourceReader struct {
	reader  io.Reader
	args    map[string]string
	records [][]string
}

func (self *StdDataSourceReader) Schema() (columns []df.Column, err error) {
	err = self.init()
	if err != nil {
		return
	}

	headerStr, ok := self.args["fmt.csv.header"]
	header := true
	if ok {
		header, err = strconv.ParseBool(headerStr)
		if err != nil {
			return
		}
	}

	columns = make([]df.Column, len(self.records[0]))
	f, _ := df.GetFormat("string")
	for i, col := range self.records[0] {
		if header {
			columns[i] = df.Column{Name: col, Format: f}
		} else {
			columns[i] = df.Column{Name: "col_" + strconv.Itoa(i), Format: f}
		}
	}
	return
}

func (self *StdDataSourceReader) Data() (data [][]interface{}, err error) {
	err = self.init()
	if err != nil {
		return
	}

	data = make([][]interface{}, len(self.records)-1)
	for i, record := range self.records[1:] {
		row := make([]interface{}, len(record))
		for j, cell := range record {
			row[j] = cell
		}

		data[i] = row
	}
	return
}

func (self *StdDataSourceReader) init() (err error) {
	if self.records != nil {
		return nil
	}
	csvReader := csv.NewReader(self.reader)
	seprator, ok := self.args["fmt.csv.sep"]
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
	self.records = records
	return
}
