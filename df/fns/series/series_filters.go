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

func IsAny(s df.Series, data ...any) (r df.Series) {
	if len(data) == 0 {
		r = s
	} else if len(data) == 1 {
		r = s.Where(func(v df.Value) bool {
			return v.Get() == data[0]
		})
	} else {
		r = s.Where(func(v df.Value) bool {
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

func IsNotAny(s df.Series, data ...any) (r df.Series) {
	if len(data) == 0 {
		r = s
	} else if len(data) == 1 {
		r = s.Where(func(v df.Value) bool {
			return v.Get() != data[0]
		})
	} else {
		r = s.Where(func(v df.Value) bool {
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

func IsNil(s df.Series) (r df.Series) {
	return IsAny(s, nil)
}

func IsNotNil(s df.Series) (r df.Series) {
	return IsNotAny(s, nil)
}
