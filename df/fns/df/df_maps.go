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

func MAsType(s df.DataFrame, t map[string]df.DataFrameSeriesFormat) (r df.DataFrame) {
	for k, v := range t {
		s, err := s.UpdateSeriesByName(k, series.MAsType(s.GetSeriesByName(k), v))
		if err != nil {
			panic(err)
		}
		r = s

	}
	return r
}

func MWhereNill(s df.DataFrame, t map[string]any) (r df.DataFrame) {
	for _, schema := range s.Schema().Series() {
		val, ok := t[schema.Name]
		if !ok {
			continue
		}
		switch schema.Format {
		case df.DateTimeFormat:
			s, err := s.UpdateSeriesByName(schema.Name, dt.WhereNil(s.GetSeriesByName(schema.Name), val.(time.Time)))
			if err != nil {
				panic(err)
			}
			r = s
		case df.IntegerFormat:
			s, err := s.UpdateSeriesByName(schema.Name, num.WhereNilInt(s.GetSeriesByName(schema.Name), val.(int64)))
			if err != nil {
				panic(err)
			}
			r = s
		case df.DoubleFormat:
			s, err := s.UpdateSeriesByName(schema.Name, num.WhereNilDouble(s.GetSeriesByName(schema.Name), val.(float64)))
			if err != nil {
				panic(err)
			}
			r = s
		case df.StringFormat:
			s, err := s.UpdateSeriesByName(schema.Name, str.WhereNil(s.GetSeriesByName(schema.Name), val.(string)))
			if err != nil {
				panic(err)
			}
			r = s
		case df.BoolFormat:
			s, err := s.UpdateSeriesByName(schema.Name, boolean.WhereNil(s.GetSeriesByName(schema.Name), val.(bool)))
			if err != nil {
				panic(err)
			}
			r = s
		}
	}
	return r
}

func MWhere(s df.DataFrame, t map[string]map[any]any) (r df.DataFrame) {
	for _, schema := range s.Schema().Series() {
		val, ok := t[schema.Name]
		if !ok {
			continue
		}
		s, err := s.UpdateSeriesByName(schema.Name, series.MWhere(s.GetSeriesByName(schema.Name), schema.Format, val))
		if err != nil {
			panic(err)
		}
		r = s
	}
	return r
}
