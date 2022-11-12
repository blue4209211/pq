package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

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

func TestNewIntExpressionSeries(t *testing.T) {
	data := []int64{
		1, 2, 3, 4, 5, 1,
	}
	s := NewIntSeriesVarArg(data...)
	s1 := s.Select(NewIntExpr().Eq(NewIntConstExpr(2)))
	assert.Equal(t, int64(1), s1.Len())

	s1 = s.Select(NewIntExpr().InConst(1, 2, 3))
	assert.Equal(t, int64(4), s1.Len())

	s1 = s.Select(NewIntExpr().InConst(1, 2, 3).EqConst(2))
	assert.Equal(t, int64(1), s1.Len())

	s1 = s.Select(NewIntExpr().BetweenConst(1, 4, df.ExprBetweenIncludeBoth))
	assert.Equal(t, int64(5), s1.Len())

	s1 = s.Select(NewIntExpr().OpConst(10, df.ExprNumOpSum))
	assert.Equal(t, int64(6), s1.Len())
	assert.Equal(t, int64(11), s1.Get(0).GetAsInt())

	snil := s.Append(NewIntSeries([]*int64{nil}))
	s1 = snil.Select(NewIntExpr().WhenNilConst(10))
	assert.Equal(t, int64(10), s1.Get(s1.Len()-1).Get())

	s1 = s.Select(NewIntExpr().WhenConst(map[int64]int64{1: 11, 2: 21}))
	assert.Equal(t, int64(11), s1.Get(0).Get())

	s1 = s.Select(NewIntExpr().NotInConst(1))
	assert.Equal(t, int64(4), s1.Len())

	s1 = s.Select(NewIntExpr().NotInConst(1))
	assert.Equal(t, int64(4), s1.Len())

}
