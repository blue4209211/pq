package series

import (
	"github.com/blue4209211/pq/df"
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

func IsAny(s df.DataFrameSeries, data ...any) (r df.DataFrameSeries) {
	if len(data) == 0 {
		r = s
	} else if len(data) == 1 {
		r = s.Where(func(v df.DataFrameSeriesValue) bool {
			return v.Get() == data[0]
		})
	} else {
		r = s.Where(func(v df.DataFrameSeriesValue) bool {
			for _, k := range data {
				if k == v.Get() {
					return true
				}
			}
			return false
		})
	}
	return r
}

func IsNotAny(s df.DataFrameSeries, data ...any) (r df.DataFrameSeries) {
	if len(data) == 0 {
		r = s
	} else if len(data) == 1 {
		r = s.Where(func(v df.DataFrameSeriesValue) bool {
			return v.Get() != data[0]
		})
	} else {
		r = s.Where(func(v df.DataFrameSeriesValue) bool {
			for _, k := range data {
				if k == v.Get() {
					return false
				}
			}
			return true
		})
	}
	return r
}

func IsNil(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return IsAny(s, nil)
}

func IsNotNil(s df.DataFrameSeries) (r df.DataFrameSeries) {
	return IsNotAny(s, nil)
}
