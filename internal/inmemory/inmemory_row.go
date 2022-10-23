package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

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

func (t *inmemoryDataFrameRow) Copy() (r df.DataFrameRow) {
	r1 := make([]any, t.Len())
	copy(r1, t.data)
	r = NewDataFrameRow(t.schema, r1)
	return r
}

func (t *inmemoryDataFrameRow) GetVal(i int) df.DataFrameSeriesValue {
	return NewDataFrameSeriesValue(t.schema.Get(i).Format, t.Get(i))
}

func (t *inmemoryDataFrameRow) GetAsString(i int) (r string) {
	return t.GetVal(i).GetAsString()
}

func (t *inmemoryDataFrameRow) GetAsInt(i int) (r int64) {
	return t.GetVal(i).GetAsInt()
}

func (t *inmemoryDataFrameRow) GetAsDouble(i int) (r float64) {
	return t.GetVal(i).GetAsDouble()
}

func (t *inmemoryDataFrameRow) GetAsBool(i int) (r bool) {
	return t.GetVal(i).GetAsBool()
}

func (t *inmemoryDataFrameRow) GetAsDatetime(i int) (r time.Time) {
	return t.GetVal(i).GetAsDatetime()
}

func (t *inmemoryDataFrameRow) GetMap() (r map[string]any) {
	r = map[string]any{}
	for i, v := range t.data {
		r[t.schema.Get(i).Name] = v
	}
	return r
}

func (t *inmemoryDataFrameRow) IsAnyNil() (r bool) {
	for _, v := range t.data {
		if v == nil {
			r = true
			break
		}
	}
	return r
}
func (t *inmemoryDataFrameRow) IsNil(i int) (r bool) {
	return t.data[i] == nil
}

// NewDataFrameRow returns new Row based on schema and data
func NewDataFrameRow(schema df.DataFrameSchema, data []any) df.DataFrameRow {
	return &inmemoryDataFrameRow{schema: schema, data: data}
}
