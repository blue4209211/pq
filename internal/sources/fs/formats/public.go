package formats

import (
	"io"
	"strings"

	"github.com/blue4209211/pq/df"
)

// FormatSource Provides interface for all the data sources
type FormatSource interface {
	Name() string
	// TODO remove reader, use filepaths, currently hard to decide when to close reader
	Reader(reader io.Reader, args map[string]string) (FormatReader, error)
	// TODO remove reader, use filepaths
	Writer(data df.DataFrame, args map[string]string) (FormatWriter, error)
	Args() map[string]string
}

// FormatReader Reads dataframe
type FormatReader interface {
	Schema() df.DataFrameSchema
	Data() *[]df.Row
}

// FormatWriter Writes dataframe to write
type FormatWriter interface {
	Write(writer io.Writer) error
}

func GetFormatHandler(fmt string) (src FormatSource, err error) {
	fmt = strings.ToLower(fmt)

	if fmt == "csv" {
		return &CsvDataSource{}, err
	} else if fmt == "json" {
		return &JsonDataSource{}, err
	} else if fmt == "xml" {
		return &XmlDataSource{}, err
	} else if fmt == "parquet" {
		return &ParquetDataSource{}, err
	} else {
		return &TextDataSource{}, err
	}
}
