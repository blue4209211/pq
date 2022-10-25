package series

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
)

func ASum(s df.DataFrameSeries) (r df.DataFrameSeriesValue) {
	if s.Schema().Format != df.IntegerFormat || s.Schema().Format != df.DoubleFormat {
		panic("only int/double format supported")
	}
	val := s.Reduce(func(dfsv1, dfsv2 df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesDoubleValue(dfsv2.GetAsDouble() + dfsv1.GetAsDouble())
	}, inmemory.NewDataFrameSeriesDoubleValue(0))
	return val
}

func AMin(s df.DataFrameSeries) (r df.DataFrameSeriesValue) {
	if s.Schema().Format != df.IntegerFormat || s.Schema().Format != df.DoubleFormat {
		panic("only int/double format supported")
	}
	val := s.Reduce(func(dfsv1, dfsv2 df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if dfsv2.GetAsDouble() < dfsv1.GetAsDouble() {
			return dfsv2
		}
		return dfsv1
	}, inmemory.NewDataFrameSeriesIntValue(0))
	return val
}

func AMax(s df.DataFrameSeries) (r df.DataFrameSeriesValue) {
	if s.Schema().Format != df.IntegerFormat || s.Schema().Format != df.DoubleFormat {
		panic("only int/double format supported")
	}
	val := s.Reduce(func(dfsv1, dfsv2 df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if dfsv2.GetAsDouble() > dfsv1.GetAsDouble() {
			return dfsv2
		}
		return dfsv1
	}, inmemory.NewDataFrameSeriesIntValue(0))
	return val
}

func AMean(s df.DataFrameSeries) (r df.DataFrameSeriesValue) {
	return inmemory.NewDataFrameSeriesDoubleValue(ASum(s).GetAsDouble() / float64(s.Len()))
}

func AMedian(s df.DataFrameSeries) (r df.DataFrameSeriesValue) {
	if s.Schema().Format != df.IntegerFormat || s.Schema().Format != df.DoubleFormat {
		panic("only int/double format supported")
	}
	s = s.Sort(df.SortOrderASC)
	middle := s.Len() / 2
	if s.Len()%2 == 0 {
		r = inmemory.NewDataFrameSeriesDoubleValue((s.Get(middle-1).GetAsDouble() + s.Get(middle).GetAsDouble()) / 2)
	} else {
		r = inmemory.NewDataFrameSeriesDoubleValue(s.Get(middle).GetAsDouble())
	}
	return r
}

func ADescribe(s df.DataFrameSeries) (r df.DataFrame) {
	return r
}

func ACountValues(s df.DataFrameSeries) (r map[df.DataFrameSeriesValue]int64) {
	s.Group().ForEach(func(a df.DataFrameSeriesValue, dfs df.DataFrameSeries) {
		r[a] = dfs.Len()
	})
	return r
}

func AUnion(s df.DataFrameSeries, s1 df.DataFrameSeries, all bool) (r df.DataFrameSeries) {
	r = s.Append(s1)
	if !all {
		r = r.Distinct()
	}
	return r
}

func AIntersection(s df.DataFrameSeries, s1 df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != s1.Schema().Format {
		panic("formats should match")
	}
	r = s.Join(df.StringFormat, s1, df.JoinCross, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
		if dfsv1.Get() == dfsv2.Get() {
			return append(r, inmemory.NewDataFrameSeriesValue(s.Schema().Format, dfsv1))
		}
		return r
	})

	return r.Distinct()
}

func ASubstract(s df.DataFrameSeries, s1 df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Schema().Format != s1.Schema().Format {
		panic("formats should match")
	}
	return r
}

func ACountNotNil(s df.DataFrameSeries) (r int64) {
	return FNotNil(s).Len()
}

func ACovariance(s df.DataFrameSeries) (r float64) {
	return r
}
