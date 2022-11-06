package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

// NewIntSeries returns a column of type int
func NewIntSeries(data []*int64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewIntValue(e)
	}
	return NewSeries(d, df.IntegerFormat)
}

func NewIntSeriesVarArg(data ...int64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		e2 := e
		d[i] = NewIntValue(&e2)
	}
	return NewSeries(d, df.IntegerFormat)
}

type intVal struct {
	data *int64
}

func (t *intVal) Schema() df.Format {
	return df.IntegerFormat
}

func (t *intVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *intVal) GetAsString() (r string) {
	v, e := df.StringFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get String Value")
	}
	return v.(string)
}

func (t *intVal) GetAsInt() (r int64) {
	return *t.data
}

func (t *intVal) GetAsDouble() (r float64) {
	v, e := df.DoubleFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Double Value")
	}
	return v.(float64)
}

func (t *intVal) GetAsBool() (r bool) {
	v, e := df.BoolFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(bool)
}

func (t *intVal) GetAsDatetime() (r time.Time) {
	v, e := df.DateTimeFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Datetime Value")
	}
	return v.(time.Time)
}

func (t *intVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *intVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewIntValue(data *int64) df.Value {
	return &intVal{data: data}
}

func NewIntValueConst(data int64) df.Value {
	return &intVal{data: &data}
}
