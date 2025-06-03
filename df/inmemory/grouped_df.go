//go:build inmemory
package inmemory

import (
	"fmt"
	"strings"

	"github.com/blue4209211/pq/df"
)

type inmemoryGroupedDataFrame struct {
	data         map[string]df.DataFrame
	keys         map[string]df.Row
	groupColumns []string
}

func (t *inmemoryGroupedDataFrame) GetGroupColumns() []string {
	s1 := make([]string, len(t.groupColumns))
	copy(s1, t.groupColumns)
	return s1
}

func (t *inmemoryGroupedDataFrame) Get(index df.Row) (d df.DataFrame) {
	return t.data[getKey(index)]
}

func (t *inmemoryGroupedDataFrame) GetKeys() (d []df.Row) {
	d = make([]df.Row, 0, len(t.data))
	for _, v := range t.keys {
		d = append(d, v)
	}
	return d
}

func (t *inmemoryGroupedDataFrame) ForEach(f func(df.Row, df.DataFrame)) {
	for k, v := range t.data {
		f(t.keys[k], v)
	}
}

func (t *inmemoryGroupedDataFrame) Map(f func(df.Row, df.DataFrame) df.DataFrame) (d df.GroupedDataFrame) {
	d1 := map[string]df.DataFrame{}
	for k, v := range t.data {
		dfr := f(t.keys[k], v)
		d1[k] = dfr
	}
	return &inmemoryGroupedDataFrame{data: d1, keys: t.keys, groupColumns: t.groupColumns}
}

func (t *inmemoryGroupedDataFrame) Where(f func(df.Row, df.DataFrame) bool) (d df.GroupedDataFrame) {
	d1 := map[string]df.DataFrame{}
	d2 := map[string]df.Row{}
	for k, v := range t.data {
		if f(t.keys[k], v) {
			d1[k] = v
			d2[k] = t.keys[k]
		}
	}
	return &inmemoryGroupedDataFrame{data: d1, keys: d2, groupColumns: t.groupColumns}
}

func (t *inmemoryGroupedDataFrame) Len() int64 {
	return int64(len(t.data))
}

// Agg performs aggregations on the groups.
func (t *inmemoryGroupedDataFrame) Agg(configs ...df.AggregationConfig) df.DataFrame {
	if len(t.data) == 0 && len(configs) == 0 {
		return NewDataframe(NewInMemorySchema("", []df.SeriesSchema{}))
	}

	// Determine output schema
	var outputSeriesSchemas []df.SeriesSchema
	var keySchema df.DataFrameSchema
	if len(t.keys) > 0 {
		// Get schema from the first key (all keys have the same schema)
		for _, kRow := range t.keys {
			keySchema = kRow.Schema()
			break
		}
		for i := 0; i < keySchema.Len(); i++ {
			outputSeriesSchemas = append(outputSeriesSchemas, keySchema.Get(i))
		}
	}

	for _, aggConfig := range configs {
		// TODO: Determine actual type based on Func and InputCol type
		// For now, count is Int64, others could be Float64 or original type
		var aggSchema df.SeriesSchema
		switch strings.ToLower(aggConfig.Func) {
		case "count":
			aggSchema = df.SeriesSchema{Name: aggConfig.OutputColName, Format: int64Format}
		case "sum", "mean", "min", "max":
			// Placeholder: This needs to be more robust, inspect input column type
			// For simplicity, assume float64 for mean, or try to match input type for others.
			// This part will be very complex in a full implementation.
			if keySchema != nil && aggConfig.InputCol != "" {
				inputColSchema, ok := keySchema.GetByName(aggConfig.InputCol)
				if !ok && len(t.data) > 0 {
					// Fallback: check the schema of the first group's data
					for _, groupDf := range t.data {
						inputColSchema, ok = groupDf.Schema().GetByName(aggConfig.InputCol)
						if ok {
							break
						}
					}
				}

				if ok {
					if strings.ToLower(aggConfig.Func) == "mean" {
						aggSchema = df.SeriesSchema{Name: aggConfig.OutputColName, Format: float64Format}
					} else {
						aggSchema = df.SeriesSchema{Name: aggConfig.OutputColName, Format: inputColSchema.Format}
					}
				} else {
					// Could not determine input type, default to float64 or error
					// For now, let's use float64 as a default for non-count if input col is specified
					aggSchema = df.SeriesSchema{Name: aggConfig.OutputColName, Format: float64Format}
				}
			} else {
				// Default for sum, min, max if InputCol is missing (though usually required)
				aggSchema = df.SeriesSchema{Name: aggConfig.OutputColName, Format: float64Format}
			}
		default:
			panic(fmt.Sprintf("unsupported aggregation function: %s", aggConfig.Func))
		}
		outputSeriesSchemas = append(outputSeriesSchemas, aggSchema)
	}
	outputSchema := NewInMemorySchema("", outputSeriesSchemas)
	outputRows := make([]df.Row, 0, len(t.data))

	for keyStr, groupDf := range t.data {
		keyRow := t.keys[keyStr]
		newRowValues := make([]df.Value, 0, outputSchema.Len())

		// Add key values
		for i := 0; i < keyRow.Len(); i++ {
			newRowValues = append(newRowValues, keyRow.Get(i))
		}

		// Calculate aggregations
		for _, aggConfig := range configs {
			var aggValue df.Value
			inputColName := aggConfig.InputCol
			aggFunc := strings.ToLower(aggConfig.Func)

			switch aggFunc {
			case "count":
				if inputColName == "" {
					aggValue = NewInt64Value(groupDf.Len())
				} else {
					colIdx := groupDf.Schema().GetIndexByName(inputColName)
					if colIdx == -1 {
						panic(fmt.Sprintf("column %s not found for count", inputColName))
					}
					count := int64(0)
					groupDf.ForEachRow(func(r df.Row) {
						if !r.IsNil(colIdx) {
							count++
						}
					})
					aggValue = NewInt64Value(count)
				}
			case "sum":
				colIdx := groupDf.Schema().GetIndexByName(inputColName)
				if colIdx == -1 {
					panic(fmt.Sprintf("column %s not found for sum", inputColName))
				}
				// This is a simplified sum, assuming numeric types that can be summed.
				// A full implementation needs type checking and dispatch.
				currentSum := 0.0 // Default to float64 for sum for now
				isFirst := true
				var sumVal df.Value

				groupDf.ForEachRow(func(r df.Row) {
					val := r.Get(colIdx)
					if val.IsNil() {
						return
					}
					// Basic sum for float64, int64. Others would need conversion or type assertion.
					switch v := val.Get().(type) {
					case float64:
						if isFirst { currentSum = 0; isFirst = false; sumVal = NewFloat64Value(0)}
						currentSum += v
						sumVal = NewFloat64Value(currentSum)
					case int64:
						if isFirst { currentSum = 0; isFirst = false; sumVal = NewInt64Value(0)}
						currentSum += float64(v) // Promote to float for generic sum
						// Determine if original type was int to store back as int if possible
						// For now, sum promotes to float64
						sumVal = NewFloat64Value(currentSum)

					case int:
						if isFirst { currentSum = 0; isFirst = false; sumVal = NewInt64Value(0)}
						currentSum += float64(v)
						sumVal = NewFloat64Value(currentSum)
					default:
						// Try to convert to float64 if possible
						fv, err := val.Schema().Convert(val.Get())
						if err == nil {
							if fvFloat, ok := fv.(float64); ok {
								if isFirst { currentSum = 0; isFirst = false; sumVal = NewFloat64Value(0) }
								currentSum += fvFloat
								sumVal = NewFloat64Value(currentSum)
								return
							}
						}
						panic(fmt.Sprintf("unsupported type for sum: %T on column %s", val.Get(), inputColName))
					}
				})
				if isFirst { // No non-nil values
					// Find the type of the column for nil value
					seriesSchema := outputSchema.GetByName(aggConfig.OutputColName)
					if seriesSchema.Format.Type() == float64Format.Type() {
						aggValue = NewFloat64Nil()
					} else if seriesSchema.Format.Type() == int64Format.Type() {
						aggValue = NewInt64Nil()
					} else {
						// Default or panic for unsupported type for nil
						aggValue = NewFloat64Nil() // Fallback
					}
				} else {
					aggValue = sumVal
				}

			case "mean":
				colIdx := groupDf.Schema().GetIndexByName(inputColName)
				if colIdx == -1 {
					panic(fmt.Sprintf("column %s not found for mean", inputColName))
				}
				sum := 0.0
				count := int64(0)
				groupDf.ForEachRow(func(r df.Row) {
					val := r.Get(colIdx)
					if !val.IsNil() {
						// Assuming numeric types convertible to float64
						// A robust solution would check types and handle errors
						floatVal, err := val.Schema().Convert(val.Get())
						if err == nil {
							switch v := floatVal.(type) {
							case float64: sum += v
							case int64:   sum += float64(v)
							case int:     sum += float64(v)
							default:
								panic(fmt.Sprintf("unsupported type for mean: %T on column %s after conversion", v, inputColName))
							}
							count++
						} else {
							panic(fmt.Sprintf("cannot convert value for mean on column %s: %v", inputColName, err))
						}
					}
				})
				if count == 0 {
					aggValue = NewFloat64Nil() // Or handle as error / NaN
				} else {
					aggValue = NewFloat64Value(sum / float64(count))
				}
			case "min", "max":
				colIdx := groupDf.Schema().GetIndexByName(inputColName)
				if colIdx == -1 {
					panic(fmt.Sprintf("column %s not found for %s", inputColName, aggFunc))
				}
				var resVal df.Value = nil
				groupDf.ForEachRow(func(r df.Row) {
					val := r.Get(colIdx)
					if val.IsNil() {
						return
					}
					if resVal == nil || resVal.IsNil() {
						resVal = val
						return
					}

					// This requires comparable types.
					// Simplified comparison logic. Real implementation needs type-specific comparisons.
					currentFloat, cfOk := convertToFloatForCompare(resVal)
					newFloat, nfOk := convertToFloatForCompare(val)

					if cfOk && nfOk {
						if aggFunc == "min" {
							if newFloat < currentFloat {
								resVal = val
							}
						} else { // max
							if newFloat > currentFloat {
								resVal = val
							}
						}
					} else {
						// Fallback to string comparison or error if types are not directly comparable as numbers
						// This is a placeholder for more robust type handling
						sCurrent := resVal.GetAsString()
						sNew := val.GetAsString()
						if aggFunc == "min" {
							if strings.Compare(sNew, sCurrent) < 0 {
								resVal = val
							}
						} else { // max
							if strings.Compare(sNew, sCurrent) > 0 {
								resVal = val
							}
						}
					}
				})

				if resVal == nil { // Group was empty or all values were nil
					// Determine the correct nil type based on the output schema for this agg column
					seriesSchema := outputSchema.GetByName(aggConfig.OutputColName)
					// This is a simplification. Ideally, df.Value itself should have a typed Nil constructor or similar.
					if seriesSchema.Format.Name() == "float64" { aggValue = NewFloat64Nil() } else
					if seriesSchema.Format.Name() == "int64" { aggValue = NewInt64Nil() } else
					if seriesSchema.Format.Name() == "string" { aggValue = NewStringNil() } else
					{ aggValue = NewFloat64Nil() /* Default nil type */ }

				} else {
					aggValue = resVal
				}

			default:
				panic(fmt.Sprintf("unsupported aggregation function: %s", aggConfig.Func))
			}
			newRowValues = append(newRowValues, aggValue)
		}
		outputRows = append(outputRows, NewInMemoryRow(outputSchema, newRowValues))
	}

	return NewDataframeFromRow(outputSchema, &outputRows)
}

// Helper function for min/max comparison, tries to convert to float64
// This is a simplification. A full solution needs proper type handling and comparison.
func convertToFloatForCompare(v df.Value) (float64, bool) {
	if v.IsNil() {
		return 0, false
	}
	switch val := v.Get().(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	// Add other numeric types if necessary
	default:
		return 0, false // Cannot convert to float64 for comparison
	}
}


func getKey(r df.Row) string {
	var b strings.Builder
	for i := 0; i < r.Len(); i++ {
		fmt.Fprintf(&b, "%v", r.Get(i).Get())
	}
	return b.String()
}

func NewGroupedDf(data df.DataFrame, others ...string) (d df.GroupedDataFrame) {
	if len(others) == 0 {
		return d
	}

	indexes := []int{}
	for _, o := range others {
		i := data.Schema().GetIndexByName(o)
		if i < 0 {
			panic("col not found - " + o)
		}
		indexes = append(indexes, i)
	}

	groupedData := map[string][]df.Row{}
	groupedRowKey := map[string]df.Row{}
	data.ForEachRow(func(dfr df.Row) {
		k := dfr.Select(indexes...)
		k1 := getKey(k)
		groupedData[k1] = append(groupedData[k1], dfr)
		groupedRowKey[k1] = k
	})

	groupedData2 := map[string]df.DataFrame{}
	for k, v := range groupedData {
		groupedData2[k] = NewDataframeFromRow(data.Schema(), &v)
	}

	return &inmemoryGroupedDataFrame{data: groupedData2, groupColumns: others, keys: groupedRowKey}
}
