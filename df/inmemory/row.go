package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

type inmemoryRow struct {
	schema *df.DataFrameSchema
	data   []df.Value
}

func (t *inmemoryRow) Schema() df.DataFrameSchema {
	return *t.schema
}

func (t *inmemoryRow) GetRaw(i int) any {
	return t.data[i].Get()
}

func (t *inmemoryRow) Len() int {
	return (*t.schema).Len()
}

func (t *inmemoryRow) Copy() (r df.Row) {
	r1 := make([]df.Value, t.Len())
	copy(r1, t.data)
	r = NewRow(t.schema, &r1)
	return r
}

func (t *inmemoryRow) Append(name string, v df.Value) (r df.Row) {
	r1 := make([]df.Value, t.Len()+1)
	copy(r1, t.data)
	r1[t.Len()] = v
	s1 := make([]df.SeriesSchema, t.Len()+1)
	copy(s1, (*t.schema).Series())
	s1[t.Len()] = df.SeriesSchema{Name: name, Format: v.Schema()}
	schema := df.NewSchema(s1)
	r = NewRow(&schema, &r1)
	return r
}

func (t *inmemoryRow) Get(i int) df.Value {
	return t.data[i]
}

func (t *inmemoryRow) GetByName(s string) df.Value {
	index := (*t.schema).GetIndexByName(s)
	if index < 0 {
		panic("col not found - " + s)
	}
	return t.Get(index)
}

func (t *inmemoryRow) GetAsString(i int) (r string) {
	return t.Get(i).GetAsString()
}

func (t *inmemoryRow) GetAsInt(i int) (r int64) {
	return t.Get(i).GetAsInt()
}

func (t *inmemoryRow) GetAsDouble(i int) (r float64) {
	return t.Get(i).GetAsDouble()
}

func (t *inmemoryRow) GetAsBool(i int) (r bool) {
	return t.Get(i).GetAsBool()
}

func (t *inmemoryRow) GetAsDatetime(i int) (r time.Time) {
	return t.Get(i).GetAsDatetime()
}

func (t *inmemoryRow) GetMap() (r map[string]df.Value) {
	r = map[string]df.Value{}
	for i, v := range t.data {
		r[(*t.schema).Get(i).Name] = v
	}
	return r
}

func (t *inmemoryRow) IsAnyNil() (r bool) {
	for _, v := range t.data {
		if v == nil || v.Get() == nil {
			r = true
			break
		}
	}
	return r
}

func (t *inmemoryRow) Select(index ...int) df.Row {
	cols := make([]df.SeriesSchema, 0, len(index))
	for _, c := range index {
		cols = append(cols, t.Schema().Get(c))
	}

	r := make([]df.Value, 0, len(index))
	for _, c := range index {
		r = append(r, t.data[c])
	}
	schema := df.NewSchema(cols)
	return NewRow(&schema, &r)
}

func (t *inmemoryRow) IsNil(i int) (r bool) {
	return t.data[i] == nil || t.data[i].IsNil()
}

// NewRow returns new Row based on schema and data
func NewRow(schema *df.DataFrameSchema, data *[]df.Value) df.Row {
	return NewRowWithCopy(schema, data, false)
}

// NewRow returns new Row based on schema and data
func NewRowWithCopy(schema *df.DataFrameSchema, data *[]df.Value, copyData bool) df.Row {
	data2 := *data
	if copyData {
		data2 = make([]df.Value, len(*data))
		copy(data2, *data)
	}
	return &inmemoryRow{schema: schema, data: data2}
}

// NewRow returns new Row based on schema and data
func NewRowFromAny(schema *df.DataFrameSchema, data *[]any) df.Row {
	data2 := make([]df.Value, len(*data))
	for i, v := range *data {
		data2[i] = NewValue((*schema).Get(i).Format, v)
	}
	return &inmemoryRow{schema: schema, data: data2}
}

// NewRow returns new Row based on schema and data
func NewRowFromMap(data *map[string]df.Value) df.Row {
	data2 := make([]df.Value, 0, len(*data))
	cols := make([]df.SeriesSchema, 0, len(*data))
	for i, v := range *data {
		data2 = append(data2, v)
		cols = append(cols, df.SeriesSchema{Name: i, Format: v.Schema()})
	}
	schema := df.NewSchema(cols)
	return &inmemoryRow{schema: &schema, data: data2}
}
