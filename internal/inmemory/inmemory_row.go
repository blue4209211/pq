package inmemory

import "github.com/blue4209211/pq/df"

type inmemoryDataFrameRow struct {
	schema df.DataFrameSchema
	data   []interface{}
}

func (t *inmemoryDataFrameRow) Schema() df.DataFrameSchema {
	return t.schema
}

func (t *inmemoryDataFrameRow) Get(i int) interface{} {
	return t.data[i]
}

func (t *inmemoryDataFrameRow) GetByName(s string) interface{} {
	index, err := t.schema.GetIndexByName(s)
	if err != nil {
		panic(err)
	}
	return t.data[index]
}

func (t *inmemoryDataFrameRow) Data() []interface{} {
	return t.data
}

func (t *inmemoryDataFrameRow) Len() int {
	return t.schema.Len()
}

// NewDataFrameRow returns new Row based on schema and data
func NewDataFrameRow(schema df.DataFrameSchema, data []interface{}) df.DataFrameRow {
	return &inmemoryDataFrameRow{schema: schema, data: data}
}
