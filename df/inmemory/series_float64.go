package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

// NewDoubleSeries returns a column of type double
func NewDoubleSeries(data []*float64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewDoubleValue(e)
	}
	return NewSeries(d, df.DoubleFormat)
}

func NewDoubleSeriesVarArg(data ...float64) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		e2 := e
		d[i] = NewDoubleValue(&e2)
	}
	return NewSeries(d, df.DoubleFormat)
}

type doubleVal struct {
	data *float64
}

func (t *doubleVal) Schema() df.Format {
	return df.DoubleFormat
}

func (t *doubleVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *doubleVal) GetAsString() (r string) {
	v, e := df.StringFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get String Value")
	}
	return v.(string)
}

func (t *doubleVal) GetAsInt() (r int64) {
	v, e := df.IntegerFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Int Value")
	}
	return v.(int64)
}

func (t *doubleVal) GetAsDouble() (r float64) {
	return *t.data
}

func (t *doubleVal) GetAsBool() (r bool) {
	v, e := df.BoolFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(bool)
}

func (t *doubleVal) GetAsDatetime() (r time.Time) {
	v, e := df.DateTimeFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Datetime Value")
	}
	return v.(time.Time)
}

func (t *doubleVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *doubleVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewDoubleValue(data *float64) df.Value {
	return &doubleVal{data: data}
}

func NewDoubleValueConst(data float64) df.Value {
	return &doubleVal{data: &data}
}
