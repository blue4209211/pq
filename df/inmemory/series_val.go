package inmemory

import (
	"fmt"
	"time"

	"github.com/blue4209211/pq/df"
)

// NewDataFrameRow returns new Row based on schema and data
func NewValue(schema df.Format, data any) df.Value {
	switch schema {
	case df.BoolFormat:
		if data == nil {
			return NewBoolValue(nil)
		}
		v := (data.(bool))
		return NewBoolValue(&v)
	case df.DateTimeFormat:
		if data == nil {
			return NewDatetimeValue(nil)
		}
		v := (data.(time.Time))
		return NewDatetimeValue(&v)
	case df.DoubleFormat:
		if data == nil {
			return NewDoubleValue(nil)
		}
		v := (data.(float64))
		return NewDoubleValue(&v)
	case df.IntegerFormat:
		if data == nil {
			return NewIntValue(nil)
		}
		v := (data.(int64))
		return NewIntValue(&v)
	case df.StringFormat:
		if data == nil {
			return NewStringValue(nil)
		}
		v := (data.(string))
		return NewStringValue(&v)
	}
	panic(fmt.Errorf("invalid format - %v", schema))
}
