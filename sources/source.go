package sources

import (
	"errors"
	"io"
	"strings"

	"github.com/blue4209211/pq/df"
)

type DataFrameSource interface {
	Name() string
	Reader(reader io.Reader, args map[string]string) (DataFrameReader, error)
	Writer(data df.DataFrame, args map[string]string) (DataFrameWriter, error)
	Args() map[string]string
}

type DataFrameReader interface {
	Schema() ([]df.Column, error)
	Data() ([][]interface{}, error)
}

type DataFrameWriter interface {
	Write(writer io.Writer) error
}

func GetSource(fmt string) (src DataFrameSource, err error) {
	fmt = strings.ToLower(fmt)

	if fmt == "csv" {
		return &CSVDataSource{}, err
	} else if fmt == "json" {
		return &JSONDataSource{}, err
	} else if fmt == "-" || fmt == "std" {
		return &StdDataSource{}, err
	}
	return src, errors.New("format not found - " + fmt)
}
