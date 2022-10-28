package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

type inmemoryDataFrameSeriesVal struct {
	schema df.Format
	data   any
}

func (t *inmemoryDataFrameSeriesVal) Schema() df.Format {
	return t.schema
}

func (t *inmemoryDataFrameSeriesVal) Get() any {
	return t.data
}

func (t *inmemoryDataFrameSeriesVal) GetAsString() (r string) {
	v, e := df.StringFormat.Convert(t.data)
	if e != nil {
		panic("unable to get String Value")
	}
	return v.(string)
}

func (t *inmemoryDataFrameSeriesVal) GetAsInt() (r int64) {
	v, e := df.IntegerFormat.Convert(t.data)
	if e != nil {
		panic("unable to get Int Value")
	}
	return v.(int64)
}

func (t *inmemoryDataFrameSeriesVal) GetAsDouble() (r float64) {
	v, e := df.DoubleFormat.Convert(t.data)
	if e != nil {
		panic("unable to get Double Value")
	}
	return v.(float64)
}

func (t *inmemoryDataFrameSeriesVal) GetAsBool() (r bool) {
	v, e := df.BoolFormat.Convert(t.data)
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(bool)
}

func (t *inmemoryDataFrameSeriesVal) GetAsDatetime() (r time.Time) {
	v, e := df.DateTimeFormat.Convert(t.data)
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(time.Time)
}

func (t *inmemoryDataFrameSeriesVal) IsNil() (r bool) {
	return t.data == nil
}

// NewDataFrameRow returns new Row based on schema and data
func NewValue(schema df.Format, data any) df.Value {
	return &inmemoryDataFrameSeriesVal{schema: schema, data: data}
}

func NewStringValue(data string) df.Value {
	return &inmemoryDataFrameSeriesVal{schema: df.StringFormat, data: data}
}

func NewIntValue(data int64) df.Value {
	return &inmemoryDataFrameSeriesVal{schema: df.IntegerFormat, data: data}
}

func NewDoubleValue(data float64) df.Value {
	return &inmemoryDataFrameSeriesVal{schema: df.DoubleFormat, data: data}
}

func NewBoolValue(data bool) df.Value {
	return &inmemoryDataFrameSeriesVal{schema: df.BoolFormat, data: data}
}

func NewDatetimeValue(data time.Time) df.Value {
	return &inmemoryDataFrameSeriesVal{schema: df.DateTimeFormat, data: data}
}
