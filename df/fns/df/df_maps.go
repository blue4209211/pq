package df

import (
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/fns/series"
	boolean "github.com/blue4209211/pq/df/fns/series/bool"
	"github.com/blue4209211/pq/df/fns/series/dt"
	"github.com/blue4209211/pq/df/fns/series/num"
	"github.com/blue4209211/pq/df/fns/series/str"
)

func AsType(s df.DataFrame, t map[string]df.Format) (r df.DataFrame) {
	for k, v := range t {
		s := s.UpdateSeriesByName(k, series.AsType(s.GetSeriesByName(k), v))
		r = s

	}
	return r
}

func WhereNill(s df.DataFrame, t map[string]any) (r df.DataFrame) {
	for _, schema := range s.Schema().Series() {
		val, ok := t[schema.Name]
		if !ok {
			continue
		}
		switch schema.Format {
		case df.DateTimeFormat:
			s := s.UpdateSeriesByName(schema.Name, dt.WhereNil(s.GetSeriesByName(schema.Name), val.(time.Time)))
			r = s
		case df.IntegerFormat:
			s := s.UpdateSeriesByName(schema.Name, num.WhereNilInt(s.GetSeriesByName(schema.Name), val.(int64)))
			r = s
		case df.DoubleFormat:
			s := s.UpdateSeriesByName(schema.Name, num.WhereNilDouble(s.GetSeriesByName(schema.Name), val.(float64)))
			r = s
		case df.StringFormat:
			s := s.UpdateSeriesByName(schema.Name, str.WhereNil(s.GetSeriesByName(schema.Name), val.(string)))
			r = s
		case df.BoolFormat:
			s := s.UpdateSeriesByName(schema.Name, boolean.WhereNil(s.GetSeriesByName(schema.Name), val.(bool)))
			r = s
		}
	}
	return r
}

func Where(s df.DataFrame, t map[string]map[any]any) (r df.DataFrame) {
	for _, schema := range s.Schema().Series() {
		val, ok := t[schema.Name]
		if !ok {
			continue
		}
		s := s.UpdateSeriesByName(schema.Name, series.Where(s.GetSeriesByName(schema.Name), schema.Format, val))
		r = s
	}
	return r
}
