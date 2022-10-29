package dt

import (
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/fns/series"
)

func IsBetween(s df.Series, min time.Time, max time.Time, between series.BetweenInclude) (r df.Series) {
	if s.Schema().Format != df.DateTimeFormat {
		panic("only supported for datetime format")
	}

	switch between {
	case series.BetweenIncludeNeighter:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return i.Before(max) && i.After(min)
		})
	case series.BetweenIncludeBoth:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return (i.Before(max) || i.Equal(max)) && (i.After(min) || i.Equal(min))
		})
	case series.BetweenIncludeLeft:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return i.Before(max) && (i.After(min) || i.Equal(min))
		})
	case series.BetweenIncludeRight:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return (i.Before(max) || i.Equal(max)) && i.After(min)
		})

	}

	return r
}

func IsCompare(s df.Series, dt time.Time, condition series.CompareCondition) (r df.Series) {
	if s.Schema().Format != df.DoubleFormat {
		panic("only supported for doble format")
	}
	switch condition {
	case series.LessThan:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return i.Before(dt)
		})
	case series.GreaterThan:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return i.After(dt)
		})
	case series.GreaterThanEqual:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return i.After(dt) || i.Equal(dt)
		})
	case series.LessThanEqual:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return i.Before(dt) || i.Equal(dt)
		})
	case series.Equal:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return i.Equal(dt)
		})
	case series.NotEqual:
		r = s.Where(func(v df.Value) bool {
			i := v.GetAsDatetime()
			return !i.Equal(dt)
		})

	}

	return r
}
