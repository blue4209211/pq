package inmemory

import (
	"testing"

	"github.com/blue4209211/pq/df"
)

func BenchmarkDfMap(t *testing.B) {
	s1 := NewIntRangeSeries(10000000)
	s2 := NewIntRangeSeries(10000000)

	d := NewDataframeWithNameFromSeries("df1", []string{"s1", "s2"}, &[]df.Series{s1, s2})

	for i := 0; i < t.N; i++ {
		d.MapRow(d.Schema(), func(r df.Row) df.Row {
			schema := d.Schema()
			return NewRow(&schema, &[]df.Value{NewIntValueConst(r.Get(0).GetAsInt() * 2), NewIntValueConst(r.Get(0).GetAsInt() + 2)})
		})
	}
}

func BenchmarkDfMapPar(t *testing.B) {
	s1 := NewIntRangeSeries(10000000)
	s2 := NewIntRangeSeries(10000000)

	d := NewDataframeWithNameFromSeries("df1", []string{"s1", "s2"}, &[]df.Series{s1, s2})
	d.(*inmemoryDataFrame).partitions = 10
	for i := 0; i < t.N; i++ {
		d.MapRow(d.Schema(), func(r df.Row) df.Row {
			schema := d.Schema()
			return NewRow(&schema, &[]df.Value{NewIntValueConst(r.Get(0).GetAsInt() * 2), NewIntValueConst(r.Get(0).GetAsInt() + 2)})
		})
	}
}

func BenchmarkDfWhere(t *testing.B) {
	s1 := NewIntRangeSeries(10000000)
	s2 := NewIntRangeSeries(10000000)

	d := NewDataframeWithNameFromSeries("df1", []string{"s1", "s2"}, &[]df.Series{s1, s2})

	for i := 0; i < t.N; i++ {
		d.WhereRow(func(r df.Row) bool {
			return r.Get(0).GetAsInt()/2 == 0
		})
	}
}

func BenchmarkDfWherePar(t *testing.B) {
	s1 := NewIntRangeSeries(10000000)
	s2 := NewIntRangeSeries(10000000)

	d := NewDataframeWithNameFromSeries("df1", []string{"s1", "s2"}, &[]df.Series{s1, s2})
	d.(*inmemoryDataFrame).partitions = 10

	for i := 0; i < t.N; i++ {
		d.WhereRow(func(r df.Row) bool {
			return r.Get(0).GetAsInt()/2 == 0
		})
	}
}
