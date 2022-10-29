package bool

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
)

func MaskNil(s df.Series, v bool) (r df.Series) {
	if s.Schema().Format != df.BoolFormat {
		panic("only supported for bool format")
	}
	r = s.Map(df.BoolFormat, func(sv df.Value) df.Value {
		if sv.Get() == nil {
			return inmemory.NewBoolValue(v)
		}
		return inmemory.NewBoolValue(sv.GetAsBool())
	})

	return r
}

func Not(bs df.Series) (r df.Series) {
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	return bs.Map(df.BoolFormat, func(dfsv df.Value) df.Value {
		return inmemory.NewBoolValue(!dfsv.GetAsBool())
	})
}

func And(bs df.Series, v bool) (r df.Series) {
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	return bs.Map(df.BoolFormat, func(dfsv df.Value) df.Value {
		return inmemory.NewBoolValue(dfsv.GetAsBool() && v)
	})
}

func Or(bs df.Series, v bool) (r df.Series) {
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	return bs.Map(df.BoolFormat, func(dfsv df.Value) df.Value {
		return inmemory.NewBoolValue(dfsv.GetAsBool() || v)
	})
}

func AndSeries(s df.Series, bs df.Series) (r df.Series) {
	if s.Len() != bs.Len() {
		panic("series len not same")
	}
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	r = s.Join(s.Schema().Format, bs, df.JoinEqui, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
		return append(r, inmemory.NewBoolValue(dfsv1.GetAsBool() && dfsv2.GetAsBool()))
	})

	return r

}

func OrSeries(s df.Series, bs df.Series) (r df.Series) {
	if s.Len() != bs.Len() {
		panic("series len not same")
	}
	if bs.Schema().Format != df.BoolFormat {
		panic("series is not bool")
	}
	r = s.Join(s.Schema().Format, bs, df.JoinEqui, func(dfsv1, dfsv2 df.Value) (r []df.Value) {
		return append(r, inmemory.NewBoolValue(dfsv1.GetAsBool() || dfsv2.GetAsBool()))
	})
	return r
}
