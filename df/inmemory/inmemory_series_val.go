package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

type inmemoryDataFrameSeriesVal struct {
	schema df.DataFrameSeriesFormat
	data   any
}

func (t *inmemoryDataFrameSeriesVal) Schema() df.DataFrameSeriesFormat {
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
func NewDataFrameSeriesValue(schema df.DataFrameSeriesFormat, data any) df.DataFrameSeriesValue {
	return &inmemoryDataFrameSeriesVal{schema: schema, data: data}
}

func NewDataFrameSeriesStringValue(data string) df.DataFrameSeriesValue {
	return &inmemoryDataFrameSeriesVal{schema: df.StringFormat, data: data}
}

func NewDataFrameSeriesIntValue(data int64) df.DataFrameSeriesValue {
	return &inmemoryDataFrameSeriesVal{schema: df.IntegerFormat, data: data}
}

func NewDataFrameSeriesDoubleValue(data float64) df.DataFrameSeriesValue {
	return &inmemoryDataFrameSeriesVal{schema: df.DoubleFormat, data: data}
}

func NewDataFrameSeriesBoolValue(data bool) df.DataFrameSeriesValue {
	return &inmemoryDataFrameSeriesVal{schema: df.BoolFormat, data: data}
}

func NewDataFrameSeriesDatetimeValue(data time.Time) df.DataFrameSeriesValue {
	return &inmemoryDataFrameSeriesVal{schema: df.BoolFormat, data: data}
}
