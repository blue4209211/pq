package df

import "github.com/blue4209211/pq/df"

func Distinct(s df.DataFrame) (r df.DataFrame) {
	return s.Distinct()
}

func Union(s df.DataFrame, s1 df.DataFrame, all bool) (r df.DataFrame) {
	r = s.Append(s1)
	if !all {
		r = r.Distinct()
	}
	return r
}

func Intersection(s df.DataFrame, s1 df.DataFrame) (r df.DataFrame) {

	if !s.Schema().Equals(s1.Schema()) {
		panic("schema is not same")
	}

	cols := map[string]string{}
	for _, s := range s.Schema().Names() {
		cols[s] = s
	}

	return s.Join(s.Schema(), s1, df.JoinEqui, cols, func(r1, r2 df.Row) []df.Row {
		return []df.Row{r1}
	}).Distinct()
}

func Substract(s df.DataFrame, s1 df.DataFrame) (r df.DataFrame) {
	return r
}
