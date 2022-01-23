package df

import (
	"strconv"
)

type inmemoryDataFrame struct {
	name   string
	schema []Column
	data   [][]interface{}
}

func (t *inmemoryDataFrame) Data() [][]interface{} {
	return t.data
}

func (t *inmemoryDataFrame) Schema() []Column {
	return t.schema
}

func (t *inmemoryDataFrame) Name() string {
	return t.name
}

func (t *inmemoryDataFrame) Column(i int) DataFrameColumn {
	colSchema := t.schema[i]
	colData := make([]interface{}, 0, len(t.data))
	for j, r := range t.data {
		colData[j] = r[i]
	}
	return &inmemoryDataFrameColumn{colSchema, colData}
}

func (t *inmemoryDataFrame) Get(i int) DataFrameRow {
	return &inmemoryDataFrameRow{t.schema, t.data[i]}
}

func (t *inmemoryDataFrame) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemoryDataFrame) ForEach(f ForeachDataframeData) {
	for _, d := range t.data {
		f(&inmemoryDataFrameRow{t.schema, d})
	}
}

type inmemoryDataFrameRow struct {
	schema []Column
	data   []interface{}
}

func (t *inmemoryDataFrameRow) Schema() []Column {
	return t.schema
}

func (t *inmemoryDataFrameRow) Get(i int) interface{} {
	return t.data[i]
}

func (t *inmemoryDataFrameRow) Data() []interface{} {
	return t.data
}

func (t *inmemoryDataFrameRow) Len() int64 {
	return int64(len(t.schema))
}

type inmemoryDataFrameColumn struct {
	schema Column
	data   []interface{}
}

func (t *inmemoryDataFrameColumn) Schema() Column {
	return t.schema
}

func (t *inmemoryDataFrameColumn) Len() int64 {
	return int64(len(t.data))
}

func (t *inmemoryDataFrameColumn) Get(i int) interface{} {
	return t.data[i]
}

func (t *inmemoryDataFrameColumn) ForEach(f ForeachColumnData) {
	for _, d := range t.data {
		f(d)
	}
}

var dfCounter = 0

// NewInmemoryDataframe Create Dataframe based on given schema and data
func NewInmemoryDataframe(cols []Column, data [][]interface{}) DataFrame {
	dfCounter = dfCounter + 1
	return NewInmemoryDataframeWithName("df_"+strconv.Itoa(dfCounter), cols, data)
}

// NewInmemoryDataframeWithName Create Dataframe based on given name, schema and data
func NewInmemoryDataframeWithName(name string, cols []Column, data [][]interface{}) DataFrame {
	return &inmemoryDataFrame{name: name, schema: cols, data: data}
}
