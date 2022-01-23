package df

import "errors"

// NewMergeDataframe Returns merged dataframe based on given dataframes and name
// Schema of new dataframe will be same as first dataframe
func NewMergeDataframe(name string, dfs ...DataFrame) (output DataFrame, err error) {
	if len(dfs) == 0 {
		return output, errors.New("Empty data")
	}

	var records [][]interface{}
	if len(dfs) == 1 {
		output = NewRenameDataframe(name, dfs[0])
	} else {
		cols := dfs[0].Schema()
		cnt := 0
		for _, d := range dfs {
			cnt = cnt + int(d.Len())
		}
		records = make([][]interface{}, cnt, cnt)

		mergeIndx := 0
		for _, df := range dfs {
			for i := 0; i < int(df.Len()); i++ {
				records[mergeIndx] = df.Get(i).Data()
				mergeIndx++
			}
		}
		output = NewInmemoryDataframeWithName(name, cols, records)
	}

	return
}
