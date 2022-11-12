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
		return inmemory.NewDoubleValueConst(dfsv2.GetAsDouble() + dfsv1.GetAsDouble())
	}, inmemory.NewDoubleValueConst(0))
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
	return inmemory.NewDoubleValueConst(Sum(s).GetAsDouble() / float64(s.Len()))
}

func Median(s df.Series) (r df.Value) {
	if !(s.Schema().Format == df.IntegerFormat || s.Schema().Format == df.DoubleFormat) {
		panic("only int/double format supported")
	}
	s = s.Sort(df.SortOrderASC)
	middle := s.Len() / 2
	if s.Len()%2 == 0 {
		r = inmemory.NewDoubleValueConst((s.Get(middle-1).GetAsDouble() + s.Get(middle).GetAsDouble()) / 2)
	} else {
		r = inmemory.NewDoubleValueConst(s.Get(middle).GetAsDouble())
	}
	return r
}

func CountDistinctValues(s df.Series) (r map[string]int64) {
	r = map[string]int64{}
	s.Group().ForEach(func(a df.Value, dfs df.Series) {
		r[a.GetAsString()] = dfs.Len()
	})
	return r
}
