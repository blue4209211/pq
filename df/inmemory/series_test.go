package inmemory

import (
	"strings"
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestNewStringSeries(t *testing.T) {

	data := []string{
		"abc", "def", "geh", "ijk", "lmn", "abc",
	}

	s := NewStringSeriesVarArg(data...)

	//len
	assert.Equal(t, int64(len(data)), s.Len())

	//where
	sf := s.Where(func(i df.Value) bool {
		return i.Get() == "abc"
	})
	assert.Equal(t, int64(2), sf.Len())

	//map
	sm := s.Map(df.StringFormat, func(i df.Value) df.Value {
		return NewStringValueConst(i.GetAsString() + "1")
	})
	assert.Equal(t, int64(len(data)), sm.Len())
	assert.Equal(t, data[1]+"1", sm.Get(1).Get())

	//flatmap
	sfm := s.FlatMap(df.StringFormat, func(i df.Value) []df.Value {
		return []df.Value{
			NewStringValueConst(i.GetAsString() + "1"),
			NewStringValueConst(i.GetAsString() + "2"),
		}
	})
	assert.Equal(t, int64(len(data)*2), sfm.Len())
	assert.Equal(t, data[0]+"2", sfm.Get(1).Get())

	//distinct
	sd := s.Distinct()
	assert.Equal(t, int64(5), sd.Len())

	//sort
	ss := s.Sort(df.SortOrderDESC)
	assert.Equal(t, "lmn", ss.Get(0).Get())

	//limit
	ss = s.Limit(1, 2)
	assert.Equal(t, int64(2), ss.Len())
	assert.Equal(t, "def", ss.Get(0).GetAsString())
	assert.Equal(t, "geh", ss.Get(1).GetAsString())

	//copy
	ss = s.Copy()
	assert.Equal(t, int64(6), ss.Len())
	assert.Equal(t, "abc", ss.Get(0).GetAsString())

	//select
	ss = s.Select(NewBoolSeriesVarArg(false, true, false, true, false, false))
	assert.Equal(t, int64(2), ss.Len())
	assert.Equal(t, "def", ss.Get(0).GetAsString())
	assert.Equal(t, "ijk", ss.Get(1).GetAsString())

	//reduce
	s1 := s.Reduce(func(v1, v2 df.Value) df.Value {
		return NewStringValueConst(v1.GetAsString() + v2.GetAsString())
	}, NewStringValueConst(""))
	assert.Equal(t, strings.Join(data, ""), s1.GetAsString())

	//group
	sg := s.Group()
	assert.Equal(t, len(sg.GetKeys()), 5)

	//append
	ss = s.Append(NewStringSeriesVarArg("1", "2"))
	assert.Equal(t, int64(8), ss.Len())
	assert.Equal(t, int64(6), s.Len())

	//join
	ss = s.Join(df.StringFormat, NewStringSeriesVarArg("1", "2"), df.JoinEqui, func(v1, v2 df.Value) []df.Value {
		return []df.Value{NewStringValueConst(v1.GetAsString() + v2.GetAsString())}
	})
	assert.Equal(t, int64(2), ss.Len())
	assert.Equal(t, "abc1", ss.Get(0).GetAsString())

	ss = s.Join(df.StringFormat, NewStringSeriesVarArg("1", "2"), df.JoinLeft, func(v1, v2 df.Value) []df.Value {
		if v2 == nil {
			return []df.Value{v1}
		}
		return []df.Value{NewStringValueConst(v1.GetAsString() + v2.GetAsString())}
	})
	assert.Equal(t, int64(6), ss.Len())
	assert.Equal(t, "abc1", ss.Get(0).GetAsString())

	ss = s.Join(df.StringFormat, NewStringSeriesVarArg("1", "2"), df.JoinCross, func(v1, v2 df.Value) []df.Value {
		if v2 == nil {
			return []df.Value{v1}
		}
		if v1 == nil {
			return []df.Value{v2}
		}
		return []df.Value{NewStringValueConst(v1.GetAsString() + v2.GetAsString())}
	})
	assert.Equal(t, int64(12), ss.Len())
	assert.Equal(t, "abc1", ss.Get(0).GetAsString())

}

func TestNewIntSeries(t *testing.T) {
	data := []int64{
		1, 2, 3, 4, 5, 1,
	}

	s := NewIntSeriesVarArg(data...)

	assert.Equal(t, int64(len(data)), s.Len())
	sf := s.Where(func(i df.Value) bool {
		return i.GetAsInt() == int64(1)
	})
	assert.Equal(t, int64(2), sf.Len())

	sm := s.Map(df.IntegerFormat, func(i df.Value) df.Value {
		return NewIntValueConst(i.GetAsInt() + 10)
	})
	assert.Equal(t, int64(len(data)), sm.Len())
	assert.Equal(t, data[1]+10, sm.Get(1).Get())

	sfm := s.FlatMap(df.IntegerFormat, func(i df.Value) []df.Value {
		return []df.Value{
			NewIntValueConst(i.GetAsInt() + 10),
			NewIntValueConst(i.GetAsInt() + 20),
		}
	})
	assert.Equal(t, int64(len(data)*2), sfm.Len())
	assert.Equal(t, data[0]+20, sfm.Get(1).Get())

	sd := s.Distinct()
	assert.Equal(t, int64(5), sd.Len())

	ss := s.Sort(df.SortOrderDESC)
	assert.Equal(t, int64(5), ss.Get(0).Get())
}

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

func TestNewDoubleSeries(t *testing.T) {
	data := []float64{
		1, 2, 3, 4, 5, 1,
	}

	s := NewDoubleSeriesVarArg(data...)

	assert.Equal(t, int64(len(data)), s.Len())
	sf := s.Where(func(i df.Value) bool {
		return i.GetAsDouble() == float64(1)
	})
	assert.Equal(t, int64(2), sf.Len())

	sm := s.Map(df.DoubleFormat, func(i df.Value) df.Value {
		return NewDoubleValueConst(i.GetAsDouble() + 10)
	})
	assert.Equal(t, int64(len(data)), sm.Len())
	assert.Equal(t, data[1]+10, sm.Get(1).Get())

	sfm := s.FlatMap(df.IntegerFormat, func(i df.Value) []df.Value {
		return []df.Value{
			NewDoubleValueConst(i.GetAsDouble() + 10),
			NewDoubleValueConst(i.GetAsDouble() + 10),
		}
	})
	assert.Equal(t, int64(len(data)*2), sfm.Len())
	assert.Equal(t, data[0]+10, sfm.Get(1).Get())

	sd := s.Distinct()
	assert.Equal(t, int64(5), sd.Len())

	ss := s.Sort(df.SortOrderDESC)
	assert.Equal(t, float64(5), ss.Get(0).Get())
}
