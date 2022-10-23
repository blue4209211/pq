package series

import (
	"strings"

	"github.com/blue4209211/pq/df"
)

func FBoolSeries(s df.DataFrameSeries, bs df.DataFrameSeries) (r df.DataFrameSeries) {
	if s.Len() != bs.Len() {
		panic("series len not same")
	}
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}

	r = s.Join(s.Schema().Format, bs, df.JoinEqui, func(dfsv1, dfsv2 df.DataFrameSeriesValue) (r []df.DataFrameSeriesValue) {
		if dfsv2.GetAsBool() {
			return append(r, dfsv1)
		}
		return r
	})

	return r
}

func FAny(s df.DataFrameSeries, data ...any) (r df.DataFrameSeries) {
	r = s.Filter(func(v df.DataFrameSeriesValue) bool {
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
	r = s.Filter(func(v df.DataFrameSeriesValue) bool {
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

func FStrContains(s df.DataFrameSeries, q string) (r df.DataFrameSeries) {
	r = s.Filter(func(v df.DataFrameSeriesValue) bool {
		return strings.Contains(v.GetAsString(), q)
	})
	return r
}

func FStrStartsWith(s df.DataFrameSeries, q string) (r df.DataFrameSeries) {
	r = s.Filter(func(v df.DataFrameSeriesValue) bool {
		return strings.HasPrefix(v.GetAsString(), q)
	})
	return r
}

func FStrEndsWith(s df.DataFrameSeries, q string) (r df.DataFrameSeries) {
	r = s.Filter(func(v df.DataFrameSeriesValue) bool {
		return strings.HasSuffix(v.GetAsString(), q)
	})
	return r
}

type FNumBetweenInclude string

const (
	FNumBetweenIncludeLeft     FNumBetweenInclude = "left"
	FNumBetweenIncludeRight    FNumBetweenInclude = "right"
	FNumBetweenIncludeBoth     FNumBetweenInclude = "both"
	FNumBetweenIncludeNeighter FNumBetweenInclude = "neighter"
)

func FNumBetweenInt(s df.DataFrameSeries, min int64, max int64, between FNumBetweenInclude) (r df.DataFrameSeries) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only supported for int format")
	}

	switch between {
	case FNumBetweenIncludeNeighter:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i < max && i > min
		})
	case FNumBetweenIncludeBoth:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i <= max && i >= min
		})
	case FNumBetweenIncludeLeft:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i < max && i >= min
		})
	case FNumBetweenIncludeRight:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i <= max && i > min
		})

	}

	return r
}

func FNumBetweenDouble(s df.DataFrameSeries, min float64, max float64, between FNumBetweenInclude) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for double format")
	}

	switch between {
	case FNumBetweenIncludeNeighter:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i < max && i > min
		})
	case FNumBetweenIncludeBoth:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i <= max && i >= min
		})
	case FNumBetweenIncludeLeft:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i < max && i >= min
		})
	case FNumBetweenIncludeRight:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i <= max && i > min
		})

	}

	return r
}

type FNumCompareCondition string

const (
	FNumLessThan         FNumCompareCondition = "lt"
	FNumGreaterThan      FNumCompareCondition = "gt"
	FNumGreaterThanEqual FNumCompareCondition = "ge"
	FNumLessThanEqual    FNumCompareCondition = "le"
	FNumEqual            FNumCompareCondition = "eq"
	FNumNotEqual         FNumCompareCondition = "ne"
)

func FNumCompareInt(s df.DataFrameSeries, q int64, condition FNumCompareCondition) (r df.DataFrameSeries) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only supported for int format")
	}
	switch condition {
	case FNumLessThan:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i < q
		})
	case FNumGreaterThan:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i > q
		})
	case FNumGreaterThanEqual:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i >= q
		})
	case FNumLessThanEqual:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i <= q
		})
	case FNumEqual:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i == q
		})
	case FNumNotEqual:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsInt()
			return i == q
		})

	}

	return r
}

func FNumCompareDouble(s df.DataFrameSeries, q float64, condition FNumCompareCondition) (r df.DataFrameSeries) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for doble format")
	}
	switch condition {
	case FNumLessThan:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i < q
		})
	case FNumGreaterThan:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i > q
		})
	case FNumGreaterThanEqual:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i >= q
		})
	case FNumLessThanEqual:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i <= q
		})
	case FNumEqual:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i == q
		})
	case FNumNotEqual:
		r = s.Filter(func(v df.DataFrameSeriesValue) bool {
			i := v.GetAsDouble()
			return i == q
		})

	}

	return r
}
