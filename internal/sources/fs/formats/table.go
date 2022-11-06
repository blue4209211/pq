package formats

import (
	"io"

	"github.com/blue4209211/pq/df"
	"github.com/olekukonko/tablewriter"
)

var tableConfig = map[string]string{}

type TableDataSource struct {
}

func (t *TableDataSource) Args() map[string]string {
	return textConfig
}

func (t *TableDataSource) Name() string {
	return "table"
}

func (t *TableDataSource) Reader(reader io.Reader, args map[string]string) (FormatReader, error) {
	panic("not supported")
}

func (t *TableDataSource) Writer(data df.DataFrame, args map[string]string) (w FormatWriter, err error) {
	return &tableDataSourceWriter{data: data, args: args}, err
}

type tableDataSourceWriter struct {
	data df.DataFrame
	args map[string]string
}

func (t *tableDataSourceWriter) Write(writer io.Writer) (err error) {
	tw := tablewriter.NewWriter(writer)
	tw.SetHeader(t.data.Schema().Names())
	tw.SetBorder(false)
	tw.SetAutoFormatHeaders(false)
	t.data.ForEachRow(func(r df.Row) {
		sa := make([]string, r.Len())
		for i := 0; i < r.Len(); i++ {
			if r.IsNil(i) {
				sa[i] = "<nil>"
			} else {
				sa[i] = r.GetAsString(i)
			}
		}
		tw.Append(sa)
	})
	tw.Render()
	return
}
