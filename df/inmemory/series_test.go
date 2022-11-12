package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
	"github.com/samber/lo"
)

func BenchmarkSeriesMap(t *testing.B) {
	s := NewIntRangeSeries(10000000)
	for i := 0; i < t.N; i++ {
		s.Map(df.IntegerFormat, func(i df.Value) df.Value {
			return NewIntValueConst(i.GetAsInt() + 10)
		})
	}
}

func BenchmarkSeriesMapPar(t *testing.B) {
	s := NewIntRangeSeries(10000000)
	s.(*genericSeries).partitions = 10
	s.Map(df.IntegerFormat, func(i df.Value) df.Value {
		return NewIntValueConst(i.GetAsInt() + 10)
	})
}

func BenchmarkSeriesWhere(t *testing.B) {
	s := NewIntRangeSeries(10000000)
	s.Where(func(i df.Value) bool {
		return i.GetAsInt()/2 == 0
	})
}

func BenchmarkWhere2(t *testing.B) {
	s := lo.RangeFrom(int64(0), 10000000)
	s2 := []int64{}
	for _, k := range s {
		if k/2 == 0 {
			s2 = append(s2, k)
		}
	}
	print(len(s2))
}

func BenchmarkSeriesWherePar(t *testing.B) {
	s := NewIntRangeSeries(10000000)
	s.(*genericSeries).partitions = 20
	s.Where(func(i df.Value) bool {
		return i.GetAsInt()/2 == 0
	})
}
