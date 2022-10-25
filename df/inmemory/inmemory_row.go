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

func (t *inmemoryDataFrameRow) GetRaw(i int) any {
	return t.data[i]
}

func (t *inmemoryDataFrameRow) Data() []df.DataFrameSeriesValue {
	data2 := make([]df.DataFrameSeriesValue, t.Len())
	for i, v := range t.data {
		data2[i] = NewDataFrameSeriesValue(t.schema.Get(i).Format, v)
	}
	return data2
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

func (t *inmemoryDataFrameRow) Append(name string, v df.DataFrameSeriesValue) (r df.DataFrameRow) {
	r1 := make([]any, t.Len()+1)
	copy(r1, t.data)
	r1[t.Len()] = v.Get()
	s1 := make([]df.SeriesSchema, t.Len()+1)
	copy(s1, t.schema.Series())
	s1[t.Len()] = df.SeriesSchema{Name: name, Format: v.Schema()}
	t.schema.Series()
	r = NewDataFrameRow(df.NewSchema(s1), r1)
	return r
}

func (t *inmemoryDataFrameRow) Get(i int) df.DataFrameSeriesValue {
	return NewDataFrameSeriesValue(t.schema.Get(i).Format, t.GetRaw(i))
}

func (t *inmemoryDataFrameRow) GetByName(s string) df.DataFrameSeriesValue {
	index, err := t.schema.GetIndexByName(s)
	if err != nil {
		panic(err)
	}
	return t.Get(index)
}

func (t *inmemoryDataFrameRow) GetAsString(i int) (r string) {
	return t.Get(i).GetAsString()
}

func (t *inmemoryDataFrameRow) GetAsInt(i int) (r int64) {
	return t.Get(i).GetAsInt()
}

func (t *inmemoryDataFrameRow) GetAsDouble(i int) (r float64) {
	return t.Get(i).GetAsDouble()
}

func (t *inmemoryDataFrameRow) GetAsBool(i int) (r bool) {
	return t.Get(i).GetAsBool()
}

func (t *inmemoryDataFrameRow) GetAsDatetime(i int) (r time.Time) {
	return t.Get(i).GetAsDatetime()
}

func (t *inmemoryDataFrameRow) GetMap() (r map[string]df.DataFrameSeriesValue) {
	r = map[string]df.DataFrameSeriesValue{}
	for i, v := range t.data {
		r[t.schema.Get(i).Name] = NewDataFrameSeriesValue(t.schema.Get(i).Format, v)
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

func (t *inmemoryDataFrameRow) Select(index ...int) df.DataFrameRow {
	cols := make([]df.SeriesSchema, 0, len(index))
	for _, c := range index {
		cols = append(cols, t.Schema().Get(c))
	}

	r := make([]any, 0, len(index))
	for _, c := range index {
		r = append(r, t.data[c])
	}
	return NewDataFrameRow(df.NewSchema(cols), r)
}

func (t *inmemoryDataFrameRow) IsNil(i int) (r bool) {
	return t.data[i] == nil
}

// NewDataFrameRow returns new Row based on schema and data
func NewDataFrameRow(schema df.DataFrameSchema, data []any) df.DataFrameRow {
	return &inmemoryDataFrameRow{schema: schema, data: data}
}