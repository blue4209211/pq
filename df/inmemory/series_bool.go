package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

// NewBoolSeries returns a column of type bool
func NewBoolSeries(data []*bool) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewBoolValue(e)
	}
	return NewSeries(d, df.BoolFormat)
}

func NewBoolSeriesVarArg(data ...bool) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewBoolValue(&e)
	}
	return NewSeries(d, df.BoolFormat)
}

type boolVal struct {
	data *bool
}

func (t *boolVal) Schema() df.Format {
	return df.BoolFormat
}

func (t *boolVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *boolVal) GetAsString() (r string) {
	v, e := df.StringFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get String Value")
	}
	return v.(string)
}

func (t *boolVal) GetAsInt() (r int64) {
	v, e := df.IntegerFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Int Value")
	}
	return v.(int64)
}

func (t *boolVal) GetAsDouble() (r float64) {
	v, e := df.DoubleFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Double Value")
	}
	return v.(float64)
}

func (t *boolVal) GetAsBool() (r bool) {
	return *t.data
}

func (t *boolVal) GetAsDatetime() (r time.Time) {
	v, e := df.DateTimeFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(time.Time)
}

func (t *boolVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *boolVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewBoolValue(data *bool) df.Value {
	return &boolVal{data: data}
}

func NewBoolValueConst(data bool) df.Value {
	return &boolVal{data: &data}
}
