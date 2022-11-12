package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestNewBoolSeries(t *testing.T) {
	data := []bool{
		true, false, true, false, true, false,
	}

	s := NewBoolSeriesVarArg(data...)

	assert.Equal(t, int64(len(data)), s.Len())
	sf := s.Where(func(i df.Value) bool {
		return i.GetAsBool() == true
	})
	assert.Equal(t, int64(3), sf.Len())

	sm := s.Map(df.BoolFormat, func(i df.Value) df.Value {
		return NewBoolValueConst(!i.GetAsBool())
	})
	assert.Equal(t, int64(len(data)), sm.Len())
	assert.Equal(t, true, sm.Get(1).Get())

	sfm := s.FlatMap(df.BoolFormat, func(i df.Value) []df.Value {
		return []df.Value{
			NewBoolValueConst(i.GetAsBool()),
			NewBoolValueConst(i.GetAsBool()),
		}
	})
	assert.Equal(t, int64(len(data)*2), sfm.Len())
	assert.Equal(t, true, sfm.Get(1).Get())

	sd := s.Distinct()
	assert.Equal(t, int64(2), sd.Len())

	ss := s.Sort(df.SortOrderDESC)
	assert.Equal(t, true, ss.Get(0).Get())

}
