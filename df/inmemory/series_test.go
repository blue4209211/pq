package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/stretchr/testify/assert"
)

func TestSeriesMapPar(t *testing.T) {
	s := NewIntRangeSeries(100000)
	s.(*genericSeries).partitions = 10
	s = s.Map(df.IntegerFormat, func(i df.Value) df.Value {
		return NewIntValueConst(i.GetAsInt() + 10)
	})
	assert.Equal(t, int64(100000), s.Len())
}

func TestSeriesWherePar(t *testing.T) {
	s := NewIntRangeSeries(100000)
	s.(*genericSeries).partitions = 10
	s = s.Where(func(i df.Value) bool {
		return i.GetAsInt()%2 == 0
	})
	assert.Equal(t, int64(50000), s.Len())
}

func TestSeriesIntersection(t *testing.T) {
	s := NewIntRangeSeries(100000)
	s1 := NewIntRangeSeries(10)
	s2 := s.Intersection(s1)
	assert.Equal(t, int64(10), s2.Len())
}

func TestSeriesExcept(t *testing.T) {
	s := NewIntRangeSeries(100000)
	s1 := NewIntRangeSeries(10)
	s2 := s.Except(s1)
	assert.Equal(t, int64(100000-10), s2.Len())
}

func TestSeriesAsFormat(t *testing.T) {
	s := NewIntRangeSeries(100000)
	s2 := s.AsFormat(df.StringFormat)
	assert.Equal(t, int64(100000), s2.Len())
	assert.Equal(t, df.StringFormat, s2.Schema().Format)
	assert.Equal(t, "0", s2.Get(0).Get())
}

func TestSeriesAsWhen(t *testing.T) {
	s := NewIntRangeSeries(100000)
	s2 := s.When(map[any]df.Value{int64(1): NewIntValueConst(-1)})
	s2 = s2.Where(func(v df.Value) bool {
		return v.GetAsInt() < 0
	})
	assert.Equal(t, int64(1), s2.Len())
}

func TestSeriesAsWhenNil(t *testing.T) {
	s := NewIntRangeSeries(100000)
	s = s.Append(NewIntSeries([]*int64{nil}))
	s2 := s.WhenNil(NewIntValueConst(-1))
	s2 = s2.Where(func(v df.Value) bool {
		return v.GetAsInt() < 0
	})
	assert.Equal(t, int64(1), s2.Len())
}

func TestSeriesAsWhenExpr(t *testing.T) {
	s := NewIntRangeSeries(100000)
	s1 := s.Expr()
	_, ok := s1.(df.IntSeriesExpr)
	assert.Equal(t, true, ok)
}
