package df

import (
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/fns/series"
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
		s, err := s.UpdateSeriesByName(schema.Name, series.MWhereNil(s.GetSeriesByName(schema.Name), schema.Format, val))
		if err != nil {
			panic(err)
		}
		r = s
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
