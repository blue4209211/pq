package inmemory

import "github.com/blue4209211/pq/df"

type inmemoryDataFrameRow struct {
	schema df.DataFrameSchema
	data   []any
}

func (t *inmemoryDataFrameRow) Schema() df.DataFrameSchema {
	return t.schema
}

func (t *inmemoryDataFrameRow) Get(i int) any {
	return t.data[i]
}

func (t *inmemoryDataFrameRow) GetByName(s string) any {
	index, err := t.schema.GetIndexByName(s)
	if err != nil {
		panic(err)
	}
	return t.data[index]
}

func (t *inmemoryDataFrameRow) Data() []any {
	return t.data
}

func (t *inmemoryDataFrameRow) Len() int {
	return t.schema.Len()
}

// NewDataFrameRow returns new Row based on schema and data
func NewDataFrameRow(schema df.DataFrameSchema, data []any) df.DataFrameRow {
	return &inmemoryDataFrameRow{schema: schema, data: data}
}
