package inmemory

import (
	"testing"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestRow(t *testing.T) {

	data := []df.Value{NewIntValueConst(1), NewDoubleValueConst(2.0), NewStringValueConst("abc"), NewBoolValueConst(false), NewDatetimeValueConst(time.Now()), NewStringValue(nil)}

	r := NewRow(df.NewSchema([]df.SeriesSchema{
		{Name: "c1", Format: df.IntegerFormat},
		{Name: "c2", Format: df.DoubleFormat},
		{Name: "c3", Format: df.StringFormat},
		{Name: "c4", Format: df.BoolFormat},
		{Name: "c5", Format: df.DateTimeFormat},
		{Name: "c6", Format: df.StringFormat},
	}), &data)

	//assert.Equal(t, data, r.Data())
	assert.Equal(t, len(data), r.Len())
	for i, c := range data {
		assert.Equal(t, c, r.Get(i))
		assert.Equal(t, c.Get(), r.GetRaw(i))
	}

	assert.Equal(t, data[0], r.GetByName("c1"))
	assert.Equal(t, int(6), r.Len())
	assert.Equal(t, int(6), len(r.GetMap()))
	assert.Equal(t, true, r.IsAnyNil())
	assert.Equal(t, true, r.IsNil(5))

	r1 := r.Copy()
	assert.Equal(t, int(6), r1.Len())

	r1 = r.Select(1, 2)
	assert.Equal(t, int(2), r1.Len())
	assert.Equal(t, 2.0, r1.GetByName("c2").Get())

	// Append(name string, v Value) Row
	r1 = r.Append("c7", NewDoubleValueConst(11.0))
	assert.Equal(t, int(7), r1.Len())
	assert.Equal(t, int(6), r.Len())

}
