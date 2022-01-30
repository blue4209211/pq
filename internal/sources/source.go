package sources

import (
	"io"
	"strings"

	"github.com/blue4209211/pq/df"
)

// DataFrameSource Provides interface for all the data sources
type DataFrameSource interface {
	Name() string
	// TODO remove reader, use filepaths, currently hard to decide when to close reader
	Reader(reader io.Reader, args map[string]string) (DataFrameReader, error)
	// TODO remove reader, use filepaths
	Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error)
	Args() map[string]string
}

type DataFrameReader interface {
	Schema() []df.Column
	Data() [][]interface{}
}

// DataFrameWriter Writes dataframe to write
type DataFrameWriter interface {
	Write(writer io.Writer) error
}

// GetSource Factory method to get source based on given string
func GetSource(fmt string) (src DataFrameSource, err error) {
	fmt = strings.ToLower(fmt)

	if fmt == "csv" {
		return &csvDataSource{}, err
	} else if fmt == "json" {
		return &jsonDataSource{}, err
	} else if fmt == "xml" {
		return &xmlDataSource{}, err
	} else if fmt == "parquet" {
		return &parquetDataSource{}, err
	} else if fmt == "-" || fmt == "std" {
		return &stdDataSource{}, err
	} else {
		return &textDataSource{}, err
	}
}
