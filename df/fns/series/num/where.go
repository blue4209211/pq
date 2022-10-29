package num

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/fns/series"
)

func IsBetweenInt(s df.Series, min int64, max int64, between series.BetweenInclude) (r df.Series) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only supported for int format")
	}

	switch between {
	case series.BetweenIncludeNeighter:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsInt()
			return i < max && i > min
		})
	case series.BetweenIncludeBoth:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsInt()
			return i <= max && i >= min
		})
	case series.BetweenIncludeLeft:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsInt()
			return i < max && i >= min
		})
	case series.BetweenIncludeRight:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsInt()
			return i <= max && i > min
		})

	}

	return r
}

func IsBetweenDouble(s df.Series, min float64, max float64, between series.BetweenInclude) (r df.Series) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for double format")
	}

	switch between {
	case series.BetweenIncludeNeighter:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i < max && i > min
		})
	case series.BetweenIncludeBoth:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i <= max && i >= min
		})
	case series.BetweenIncludeLeft:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i < max && i >= min
		})
	case series.BetweenIncludeRight:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i <= max && i > min
		})

	}

	return r
}

func IsCompareInt(s df.Series, q int64, condition series.CompareCondition) (r df.Series) {
	if s.Schema().Format != df.IntegerFormat {
		panic("only supported for int format")
	}
	switch condition {
	case series.LessThan:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsInt()
			return i < q
		})
	case series.GreaterThan:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsInt()
			return i > q
		})
	case series.GreaterThanEqual:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsInt()
			return i >= q
		})
	case series.LessThanEqual:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsInt()
			return i <= q
		})
	case series.Equal:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsInt()
			return i == q
		})
	case series.NotEqual:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsInt()
			if v == nil || v.IsNil() {
				return false
			}
			return i != q
		})

	}

	return r
}

func IsCompareDouble(s df.Series, q float64, condition series.CompareCondition) (r df.Series) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for doble format")
	}
	switch condition {
	case series.LessThan:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i < q
		})
	case series.GreaterThan:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i > q
		})
	case series.GreaterThanEqual:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i >= q
		})
	case series.LessThanEqual:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i <= q
		})
	case series.Equal:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i == q
		})
	case series.NotEqual:
		r = s.Where(func(v df.Value) bool {
			if v == nil || v.IsNil() {
				return false
			}
			i := v.GetAsDouble()
			return i != q
		})

	}

	return r
}
