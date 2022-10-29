package series

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
)

func Sum(s df.Series) (r df.Value) {
	if !(s.Schema().Format == df.IntegerFormat || s.Schema().Format == df.DoubleFormat) {
		panic("only int/double format supported")
	}
	val := s.Reduce(func(dfsv1, dfsv2 df.Value) df.Value {
		return inmemory.NewDoubleValue(dfsv2.GetAsDouble() + dfsv1.GetAsDouble())
	}, inmemory.NewDoubleValue(0))
	return val
}

func Min(s df.Series) (r df.Value) {
	if !(s.Schema().Format == df.IntegerFormat || s.Schema().Format == df.DoubleFormat || s.Schema().Format == df.DateTimeFormat) {
		panic("only int/double/datetime format supported")
	}
	val := s.Reduce(func(dfsv1, dfsv2 df.Value) df.Value {
		if dfsv1 == nil || dfsv1.IsNil() {
			return dfsv2
		}
		if dfsv2.GetAsDouble() < dfsv1.GetAsDouble() {
			return dfsv2
		}
		return dfsv1
	}, inmemory.NewValue(s.Schema().Format, nil))
	return val
}

func Max(s df.Series) (r df.Value) {
	if !(s.Schema().Format == df.IntegerFormat || s.Schema().Format == df.DoubleFormat || s.Schema().Format == df.DateTimeFormat) {
		panic("only int/double/datetime format supported")
	}
	val := s.Reduce(func(dfsv1, dfsv2 df.Value) df.Value {
		if dfsv1 == nil || dfsv1.IsNil() {
			return dfsv2
		}
		if dfsv2.GetAsDouble() > dfsv1.GetAsDouble() {
			return dfsv2
		}
		return dfsv1
	}, inmemory.NewValue(s.Schema().Format, nil))
	return val
}

func Mean(s df.Series) (r df.Value) {
	return inmemory.NewDoubleValue(Sum(s).GetAsDouble() / float64(s.Len()))
}

func Median(s df.Series) (r df.Value) {
	if !(s.Schema().Format == df.IntegerFormat || s.Schema().Format == df.DoubleFormat) {
		panic("only int/double format supported")
	}
	s = s.Sort(df.SortOrderASC)
	middle := s.Len() / 2
	if s.Len()%2 == 0 {
		r = inmemory.NewDoubleValue((s.Get(middle-1).GetAsDouble() + s.Get(middle).GetAsDouble()) / 2)
	} else {
		r = inmemory.NewDoubleValue(s.Get(middle).GetAsDouble())
	}
	return r
}

func Describe(s df.Series) (r df.DataFrame) {
	return r
}

func CountDistinctValues(s df.Series) (r map[string]int64) {
	r = map[string]int64{}
	s.Group().ForEach(func(a df.Value, dfs df.Series) {
		r[a.GetAsString()] = dfs.Len()
	})
	return r
}

func Union(s df.Series, s1 df.Series, all bool) (r df.Series) {
	r = s.Append(s1)
	if !all {
		r = r.Distinct()
	}
	return r
}

func Intersection(s df.Series, s1 df.Series) (r df.Series) {
	if s.Schema().Format != s1.Schema().Format {
		panic("formats should match")
	}
	r = s.Join(df.StringFormat, s1, df.JoinCross, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
		if dfsv1.Get() == dfsv2.Get() {
			return append(r, inmemory.NewValue(s.Schema().Format, dfsv1.Get()))
		}
		return r
	})

	return r.Distinct()
}

func Substract(s df.Series, s1 df.Series) (r df.Series) {
	if s.Schema().Format != s1.Schema().Format {
		panic("formats should match")
	}
	return r
}

func CountNotNil(s df.Series) (r int64) {
	return HasNotNil(s).Len()
}

func Covariance(s df.Series) (r float64) {
	return r
}
