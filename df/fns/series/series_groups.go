package series

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
)

func AMinInt(s df.DataFrameSeries) (r int64) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only int format supported")
	}
	val := s.Reduce(func(dfsv1, dfsv2 df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if dfsv2.GetAsInt() < dfsv1.GetAsInt() {
			return dfsv2
		}
		return dfsv1
	}, inmemory.NewDataFrameSeriesIntValue(0))
	return val.GetAsInt()
}

func AMaxInt(s df.DataFrameSeries) (r int64) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only int format supported")
	}
	val := s.Reduce(func(dfsv1, dfsv2 df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		if dfsv2.GetAsInt() > dfsv1.GetAsInt() {
			return dfsv2
		}
		return dfsv1
	}, inmemory.NewDataFrameSeriesIntValue(0))
	return val.GetAsInt()
}

func AMedian(s df.DataFrameSeries) (r any) {
	return r
}

func AMean(s df.DataFrameSeries) (r any) {
	return r
}

func ACumSum(s df.DataFrameSeries) (r any) {
	return r
}

func AIsUnique(s df.DataFrameSeries) (r bool) {
	return r
}

func AHasNil(s df.DataFrameSeries) (r bool) {
	return r
}

func ADescribe(s df.DataFrameSeries) (r df.DataFrame) {
	return r
}

func ACountValues(s df.DataFrameSeries) (r map[any]int64) {
	s.Group().ForEach(func(a any, dfs df.DataFrameSeries) {
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
