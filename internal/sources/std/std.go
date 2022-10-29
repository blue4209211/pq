package std

import (
	"context"
	"os"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/blue4209211/pq/internal/sources/fs/formats"
)

// ConfigStdType source format for StdIn/Out
const ConfigStdType = "std.type"

var stdConfig = map[string]string{
	ConfigStdType: "json",
}

// DataSource datsource to handle Std in/out
type DataSource struct {
}

//IsSupported IsSupported returns supported protocols by std sources
func (t *DataSource) IsSupported(protocol string) bool {
	return protocol == "" || protocol == "std"
}

func (t *DataSource) Read(context context.Context, url string, args map[string]string) (data df.DataFrame, err error) {
	streamFormat, ok := args[ConfigStdType]
	if !ok {
		streamFormat = "json"
	}
	handler, err := formats.GetFormatHandler(streamFormat)
	if err != nil {
		return data, err
	}

	reader, err := handler.Reader(os.Stdin, args)
	if err != nil {
		return data, err
	}

	data2 := reader.Data()
	return inmemory.NewDataframeFromRowAndName("stdin", reader.Schema(), data2), err
}

func (t *DataSource) Write(context context.Context, data df.DataFrame, path string, args map[string]string) (err error) {
	streamFormat, ok := args[ConfigStdType]
	if !ok {
		streamFormat = "json"
	}
	handler, err := formats.GetFormatHandler(streamFormat)
	if err != nil {
		return err
	}

	writer, err := handler.Writer(data, args)
	if err != nil {
		return err
	}

	return writer.Write(os.Stdout)
}
