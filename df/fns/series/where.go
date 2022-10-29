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

func HasAny(s df.Series, data ...any) (r df.Series) {
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

func HasAnyBool(s df.Series, data ...any) (r bool) {
	return HasAny(s, data...).Len() > 0
}

func HasNotAny(s df.Series, data ...any) (r df.Series) {
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

func HasNotAnyBool(s df.Series, data ...any) (r bool) {
	return HasNotAny(s, data...).Len() > 0
}

func HasNil(s df.Series) (r df.Series) {
	return HasAny(s, nil)
}

func HasNilBool(s df.Series) (r bool) {
	return HasAny(s, nil).Len() > 0
}

func HasNotNil(s df.Series) (r df.Series) {
	return HasNotAny(s, nil)
}

func HasNotNilBool(s df.Series) (r bool) {
	return HasNotAny(s, nil).Len() > 0
}
