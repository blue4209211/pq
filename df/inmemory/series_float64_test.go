package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

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

func TestNewDoubleExpressionSeries(t *testing.T) {
	data := []float64{
		1, 2, 3, 4, 5, 1,
	}
	s := NewDoubleSeriesVarArg(data...)
	s1 := s.Select(NewDoubleExpr().Eq(NewDoubleConstExpr(2)))
	assert.Equal(t, int64(1), s1.Len())

	s1 = s.Select(NewDoubleExpr().InConst(1, 2, 3))
	assert.Equal(t, int64(4), s1.Len())

	s1 = s.Select(NewDoubleExpr().InConst(1, 2, 3).EqConst(2))
	assert.Equal(t, int64(1), s1.Len())

	s1 = s.Select(NewDoubleExpr().BetweenConst(1, 4, df.ExprBetweenIncludeBoth))
	assert.Equal(t, int64(5), s1.Len())

	s1 = s.Select(NewDoubleExpr().OpConst(10, df.ExprNumOpSum))
	assert.Equal(t, int64(6), s1.Len())
	assert.Equal(t, float64(11), s1.Get(0).GetAsDouble())

	snil := s.Append(NewDoubleSeries([]*float64{nil}))
	s1 = snil.Select(NewDoubleExpr().WhenNilConst(10))
	assert.Equal(t, float64(10), s1.Get(s1.Len()-1).Get())

	s1 = s.Select(NewDoubleExpr().WhenConst(map[float64]float64{1: 11, 2: 21}))
	assert.Equal(t, float64(11), s1.Get(0).Get())

	s1 = s.Select(NewDoubleExpr().NotInConst(1))
	assert.Equal(t, int64(4), s1.Len())

	s1 = s.Select(NewDoubleExpr().NotInConst(1))
	assert.Equal(t, int64(4), s1.Len())

}
