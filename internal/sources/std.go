package sources

import (
	"errors"
	"io"

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
	stdDataSource := stdDataSourceReader{args: args}
	err := stdDataSource.init(reader)
	return &stdDataSource, err
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
	args    map[string]string
	records [][]string
}

func (t *stdDataSourceReader) Schema() (columns []df.Column) {
	return
}

func (t *stdDataSourceReader) Data() (data [][]interface{}) {
	return
}

func (t *stdDataSourceReader) init(reader io.Reader) (err error) {
	panic("Not supported")
}
