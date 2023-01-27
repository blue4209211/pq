package formats

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/blue4209211/pq/internal/log"
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

		for i, c := range schema.Series() {
			cols[i] = c.Name
		}
		csvWriter.Write(cols)
	}

	for i := int64(0); i < t.data.Len(); i++ {
		rowInterface := t.data.GetRow(i)
		row := make([]string, rowInterface.Len())
		for j := 0; j < rowInterface.Len(); j++ {
			str := ""
			if !rowInterface.IsNil(j) {
				str = rowInterface.GetAsString(j)
			}
			row[j] = str
		}
		csvWriter.Write(row)
	}
	return
}

type csvDataSourceReader struct {
	args     map[string]string
	isHeader bool
	schema   df.DataFrameSchema
	data     []df.Row
}

func (t *csvDataSourceReader) Schema() df.DataFrameSchema {
	return t.schema
}

func (t *csvDataSourceReader) Data() *[]df.Row {
	return &t.data
}

func (t *csvDataSourceReader) init(reader io.Reader) (err error) {
	startTime := time.Now()

	csvReader := csv.NewReader(reader)
	seprator, err := csvGetColSeprator(t.args)
	if err != nil {
		return
	}
	csvReader.Comma = seprator
	isHeader, err := csvIsHeaderEnabled(t.args)
	if err != nil {
		return
	}
	t.isHeader = isHeader
	t.data = []df.Row{}

	count := 0
	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		if count == 0 {
			columns := make([]df.SeriesSchema, len(record))
			f, _ := df.GetFormat("string")
			for i, col := range record {
				if t.isHeader {
					columns[i] = df.SeriesSchema{Name: col, Format: f}
				} else {
					columns[i] = df.SeriesSchema{Name: "c" + strconv.Itoa(i), Format: f}
				}
			}
			t.schema = df.NewSchema(columns)
			if isHeader {
				count = count + 1
				continue
			}
		}

		row := make([]df.Value, len(record))
		for j, cell := range record {
			row[j] = inmemory.NewStringValueConst(cell)
		}
		t.data = append(t.data, inmemory.NewRow(&t.schema, &row))
		count = count + 1
	}

	log.Debug("time to read csv data ", time.Since(startTime).String()+" records ", len(t.data))
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
