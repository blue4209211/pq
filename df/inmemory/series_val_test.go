package inmemory

import (
	"testing"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestNewDataFrameVal(t *testing.T) {
	val := NewIntValueConst(1)
	assert.Equal(t, df.IntegerFormat, val.Schema())
	assert.Equal(t, int64(1), val.GetAsInt())
	assert.Equal(t, int64(1), val.Get())
	assert.Equal(t, "1", val.GetAsString())
	assert.Equal(t, 1.0, val.GetAsDouble())

	val = NewDoubleValueConst(1.0)
	assert.Equal(t, df.DoubleFormat, val.Schema())
	assert.Equal(t, float64(1.0), val.GetAsDouble())
	assert.Equal(t, int64(1), val.GetAsInt())
	assert.Equal(t, float64(1), val.Get())
	//assert.Equal(t, "1.0", val.GetAsString())

	val = NewBoolValueConst(true)
	assert.Equal(t, df.BoolFormat, val.Schema())
	assert.Equal(t, true, val.GetAsBool())
	assert.Equal(t, 1.0, val.GetAsDouble())
	assert.Equal(t, int64(1), val.GetAsInt())
	assert.Equal(t, "true", val.GetAsString())

	dt := time.Now()
	val = NewDatetimeValueConst(dt)
	assert.Equal(t, df.DateTimeFormat, val.Schema())
	assert.Equal(t, dt, val.GetAsDatetime())
	assert.Equal(t, float64(dt.UnixMilli()), val.GetAsDouble())
	assert.Equal(t, dt.UnixMilli(), val.GetAsInt())
	assert.Equal(t, dt.String(), val.GetAsString())

	val = NewStringValue(nil)
	assert.Equal(t, df.StringFormat, val.Schema())
	assert.Equal(t, true, val.IsNil())
	assert.Equal(t, nil, val.Get())
	//TODO assert panic
	//assert.Equal(t, "", val.GetAsString())
}

func TestNewDataFrameValEqual(t *testing.T) {
	val := NewIntValueConst(1)
	val2 := NewIntValueConst(1)
	assert.Equal(t, val, val2)

	val = NewDoubleValueConst(1)
	val2 = NewDoubleValueConst(1)
	assert.Equal(t, val, val2)

	dt := time.Now()
	val = NewDatetimeValue(&dt)
	val2 = NewDatetimeValue(&dt)
	assert.Equal(t, val, val2)

}
