package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

// NewDatetimeSeries returns a column of type double
func NewDatetimeSeries(data []*time.Time) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewDatetimeValue(e)
	}
	return NewSeries(d, df.DateTimeFormat)
}

func NewDatetimeSeriesVarArg(data ...time.Time) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		e2 := e
		d[i] = NewDatetimeValue(&e2)
	}
	return NewSeries(d, df.DateTimeFormat)
}

type timeVal struct {
	data *time.Time
}

func (t *timeVal) Schema() df.Format {
	return df.DateTimeFormat
}

func (t *timeVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *timeVal) GetAsString() (r string) {
	v, e := df.StringFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get String Value")
	}
	return v.(string)
}

func (t *timeVal) GetAsInt() (r int64) {
	v, e := df.IntegerFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Int Value")
	}
	return v.(int64)
}

func (t *timeVal) GetAsDouble() (r float64) {
	v, e := df.DoubleFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Double Value")
	}
	return v.(float64)
}

func (t *timeVal) GetAsBool() (r bool) {
	v, e := df.BoolFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(bool)
}

func (t *timeVal) GetAsDatetime() (r time.Time) {
	return *t.data
}

func (t *timeVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *timeVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewDatetimeValue(data *time.Time) df.Value {
	return &timeVal{data: data}
}

func NewDatetimeValueConst(data time.Time) df.Value {
	return &timeVal{data: &data}
}
