package inmemory

import (
	"errors"

	"github.com/blue4209211/pq/df"
)

// NewMergeDataframe Returns merged dataframe based on given dataframes and name
// Schema of new dataframe will be same as first dataframe
func NewMergeDataframe(name string, dfs ...df.DataFrame) (output df.DataFrame, err error) {
	if len(dfs) == 0 {
		return output, errors.New("Empty data")
	}

	var records [][]any
	if len(dfs) == 1 {
		output = dfs[0].Rename(name, false)
	} else {
		schema := dfs[0].Schema()
		cnt := 0
		for _, d := range dfs {
			cnt = cnt + int(d.Len())
		}
		records = make([][]any, cnt, cnt)

		mergeIndx := 0
		for _, df := range dfs {
			for i := int64(0); i < df.Len(); i++ {
				records[mergeIndx] = df.Get(i).Data()
				mergeIndx++
			}
		}
		output = NewDataframeWithName(name, schema.Columns(), records)
	}

	return
}
