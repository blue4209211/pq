package sources

import (
	"errors"
	"io"
	"strings"

	"github.com/blue4209211/pq/df"
)

// DataFrameSource Provides interface for all the data sources
type DataFrameSource interface {
	Name() string
	Reader(reader io.Reader, args map[string]string) (DataFrameReader, error)
	Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error)
	Args() map[string]string
}

// DataFrameReader Provides interface to Read data/schema from source
type DataFrameReader interface {
	Schema() ([]df.Column, error)
	Data() ([][]interface{}, error)
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
	} else if fmt == "-" || fmt == "std" {
		return &stdDataSource{}, err
	}
	return src, errors.New("format not found - " + fmt)
}
