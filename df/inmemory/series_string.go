package inmemory

import (
	"time"

	"github.com/blue4209211/pq/df"
)

// NewStringSeries returns a column of type string
func NewStringSeries(data []*string) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		d[i] = NewStringValue(e)
	}
	return NewSeries(d, df.StringFormat)
}

func NewStringSeriesVarArg(data ...string) df.Series {
	d := make([]df.Value, len(data))
	for i, e := range data {
		e2 := e
		d[i] = NewStringValue(&e2)
	}
	return NewSeries(d, df.StringFormat)
}

type stringVal struct {
	data *string
}

func (t *stringVal) Schema() df.Format {
	return df.StringFormat
}

func (t *stringVal) Get() any {
	if t.data == nil {
		return nil
	}
	return *(t.data)
}

func (t *stringVal) GetAsString() (r string) {
	return *t.data
}

func (t *stringVal) GetAsInt() (r int64) {
	v, e := df.IntegerFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Int Value")
	}
	return v.(int64)
}

func (t *stringVal) GetAsDouble() (r float64) {
	v, e := df.DoubleFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Double Value")
	}
	return v.(float64)
}

func (t *stringVal) GetAsBool() (r bool) {
	v, e := df.BoolFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(bool)
}

func (t *stringVal) GetAsDatetime() (r time.Time) {
	v, e := df.DateTimeFormat.Convert(*(t.data))
	if e != nil {
		panic("unable to get Bool Value")
	}
	return v.(time.Time)
}

func (t *stringVal) IsNil() (r bool) {
	return t.data == nil
}

func (t *stringVal) Equals(other df.Value) (r bool) {
	return other != nil && t.Get() == other.Get() && t.Schema() == other.Schema()
}

func NewStringValue(data *string) df.Value {
	return &stringVal{data: data}
}

func NewStringValueConst(data string) df.Value {
	return &stringVal{data: &data}
}
