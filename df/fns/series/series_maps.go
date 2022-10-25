package series

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
)

func Where(s df.Series, f df.Format, v map[any]any) (r df.Series) {
	r = s.Map(f, func(sv df.Value) df.Value {
		k, ok := v[sv.Get()]
		if ok {
			return inmemory.NewValue(f, k)
		}
		return sv
	})
	return r
}

func AsType(s df.Series, t df.Format) (r df.Series) {
	return s.Map(t, func(dfsv df.Value) df.Value {
		v, e := t.Convert(dfsv.Get())
		if e != nil {
			v = nil
		}
		return inmemory.NewValue(t, v)
	})
}
