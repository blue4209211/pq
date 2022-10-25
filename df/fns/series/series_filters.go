package series

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/inmemory"
)

type CompareCondition string

const (
	LessThan         CompareCondition = "lt"
	GreaterThan      CompareCondition = "gt"
	GreaterThanEqual CompareCondition = "ge"
	LessThanEqual    CompareCondition = "le"
	Equal            CompareCondition = "eq"
	NotEqual         CompareCondition = "ne"
)

type BetweenInclude string

const (
	BetweenIncludeLeft     BetweenInclude = "left"
	BetweenIncludeRight    BetweenInclude = "right"
	BetweenIncludeBoth     BetweenInclude = "both"
	BetweenIncludeNeighter BetweenInclude = "neighter"
)

func FBoolAnd(s df.DataFrameSeries, bs df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Len() != bs.Len() {
		panic("series len not same")
	}
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	r = s.Join(s.Schema().Format, bs, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
		return append(r, inmemory.NewDataFrameSeriesBoolValue(dfsv1.GetAsBool() && dfsv2.GetAsBool()))
	})

	return r

}

func FBoolOr(s df.DataFrameSeries, bs df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Len() != bs.Len() {
		panic("series len not same")
	}
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	r = s.Join(s.Schema().Format, bs, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
		return append(r, inmemory.NewDataFrameSeriesBoolValue(dfsv1.GetAsBool() || dfsv2.GetAsBool()))
	})
	return r
}

func FBoolNot(bs df.DataFrameSeries) (r df.DataFrameSeries) {
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	return bs.Map(df.BoolFormat, func(dfsv df.DataFrameSeriesValue) df.DataFrameSeriesValue {
		return inmemory.NewDataFrameSeriesBoolValue(!dfsv.GetAsBool())
	})
}

func FAny(s df.DataFrameSeries, data ...any) (r df.DataFrameSeries) {
	r = s.Where(func(v df.DataFrameSeriesValue) bool {
		for _, k := range data {
			if k == v.Get() {
				return true
			}
		}
		return false
	})
	return r
}

func FNotAny(s df.DataFrameSeries, data ...any) (r df.DataFrameSeries) {
	r = s.Where(func(v df.DataFrameSeriesValue) bool {
		for _, k := range data {
			if k == v.Get() {
				return false
			}
		}
		return true
	})
	return r
}

func FNil(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return FAny(s, nil)
}

func FNotNil(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return FNotAny(s, nil)
}
