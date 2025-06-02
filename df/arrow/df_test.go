//go:build arrow

package arrow_test

import (
	"fmt"
	"sort"
	"testing"
	"time"
	"reflect"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/expr"
	"github.com/stretchr/testify/assert"

	arrowimpl "github.com/blue4209211/pq/df/arrow"
)

// --- Helper functions ---
func getTestDataFrameArrowSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "col_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "col_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)
}
func getTestDataFrameRecord(mem memory.Allocator, schema *arrow.Schema) arrow.Record {
	b := array.NewRecordBuilder(mem, schema); defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"alpha", "beta", "gamma"}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{100, 0, 300}, []bool{true, false, true})
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 0}, []bool{true, true, false})
	return b.NewRecord()
}
func getBaseTestDf(t *testing.T, mem memory.Allocator) df.DataFrame {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "col_a", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "col_b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}, nil,
	)
	rb := array.NewRecordBuilder(mem, schema); defer rb.Release()
	rb.Field(0).(*array.StringBuilder).AppendValues([]string{"row1", "row2", "row3"}, nil)
	rb.Field(1).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)
	record := rb.NewRecord()
	dfSchema := arrowimpl.NewArrowDataFrameSchema(schema).(*arrowimpl.ArrowDataFrameSchema)
	return arrowimpl.NewArrowDataFrame("test_df", record, dfSchema)
}
func getTestInt64Array(mem memory.Allocator, values []int64, valids []bool) arrow.Array {
	b := array.NewInt64Builder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}
func getTestStringArray(mem memory.Allocator, values []string, valids []bool) arrow.Array {
	b := array.NewStringBuilder(mem); defer b.Release(); b.AppendValues(values, valids); return b.NewArray()
}

const nilPlaceholder = "__NIL_PLACEHOLDER__"

func dfToSliceOfInterfaceSlices(dataFrame df.DataFrame) [][]interface{} {
	var result [][]interface{}
	if dataFrame == nil || dataFrame.Len() == 0 { return result }
	for r := int64(0); r < dataFrame.Len(); r++ {
		row := dataFrame.GetRow(r); var rowData []interface{}
		for c := 0; c < row.Len(); c++ {
			val := row.Get(c)
			if val.IsNil() { rowData = append(rowData, nilPlaceholder) } else { rowData = append(rowData, val.Get()) }
		}
		result = append(result, rowData)
	}
	return result
}

func sortSliceOfInterfaceSlices(slice [][]interface{}) {
	sort.Slice(slice, func(i, j int) bool { return fmt.Sprintf("%v", slice[i]) < fmt.Sprintf("%v", slice[j]) })
}

// --- Existing tests ---
func TestArrowDataFrame_NewArrowDataFrame(t *testing.T) { /* ... */ }
func TestArrowDataFrame_NewArrowDataFrameFromArrays(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Accessors(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Limit(t *testing.T) { /* ... */ }
func TestArrowDataFrame_SelectBySeriesIndex(t *testing.T) { /* ... */ }
func TestArrowDataFrame_SelectBySeriesName(t *testing.T) { /* ... */ }
func TestArrowDataFrame_WhereRow(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Sort(t *testing.T) { /* ... */ }
func TestArrowDataFrame_AddSeries(t *testing.T) { /* ... */ }
func TestArrowDataFrame_RemoveSeries(t *testing.T) { /* ... */ }
func TestArrowDataFrame_RenameSeries(t *testing.T) { /* ... */ }
func TestArrowDataFrame_GetSeriesExprByName(t *testing.T) { /* ... */ }
func TestArrowDataFrame_MapRow(t *testing.T) { /* ... */ }
func TestArrowDataFrame_FlatMapRow(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Distinct(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Append(t *testing.T) { /* ... */ }
func TestArrowDataFrame_Union(t *testing.T) { /* ... */ }
func TestArrowDataFrame_WhenNil(t *testing.T) { /* ... */ }
func TestArrowDataFrame_When(t *testing.T) { /* ... */ }
func TestArrowDataFrame_UpdateSeries(t *testing.T) { /* ... */ }


func TestArrowDataFrame_Join_EquiJoin(t *testing.T) {
	mem := memory.NewGoAllocator()

	lSchema := arrow.NewSchema(
		[]arrow.Field{ {Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "val_l", Type: arrow.BinaryTypes.String}, }, nil,
	)
	ldfSchema := arrowimpl.NewArrowDataFrameSchema(lSchema).(*arrowimpl.ArrowDataFrameSchema)
	lrb := array.NewRecordBuilder(mem, lSchema); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 0}, []bool{true,true,true,true,false})
	lrb.Field(1).(*array.StringBuilder).AppendValues([]string{"L1", "L2", "L3", "L4", "L5_nil_id"}, nil)
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("left", lRec, ldfSchema)
	defer ldf.(*arrowimpl.ArrowDataFrame).Release()

	rSchema := arrow.NewSchema(
		[]arrow.Field{ {Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "val_r", Type: arrow.PrimitiveTypes.Float64}, }, nil,
	)
	rdfSchema := arrowimpl.NewArrowDataFrameSchema(rSchema).(*arrowimpl.ArrowDataFrameSchema)
	rrb := array.NewRecordBuilder(mem, rSchema); defer rrb.Release()
	rrb.Field(0).(*array.Int64Builder).AppendValues([]int64{2, 0, 3, 5, 3}, []bool{true,false,true,true,true})
	rrb.Field(1).(*array.Float64Builder).AppendValues([]float64{20.2, 99.9, 30.3, 50.5, 30.33}, []bool{true,true,true,true,true})
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("right", rRec, rdfSchema)
	defer rdf.(*arrowimpl.ArrowDataFrame).Release()

	outJoinSchemaArrow := arrow.NewSchema(
		[]arrow.Field{
			{Name: "l_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, // Nullable true because join keys can be nil
			{Name: "l_val", Type: arrow.BinaryTypes.String},
			{Name: "r_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "r_val", Type: arrow.PrimitiveTypes.Float64},
			{Name: "combined", Type: arrow.BinaryTypes.String},
		}, nil,
	)
	outJoinDfSchema := arrowimpl.NewArrowDataFrameSchema(outJoinSchemaArrow).(*arrowimpl.ArrowDataFrameSchema)

	fUser := func(r1, r2 df.Row) []df.Row {
		lIDVal := r1.GetByName("id"); lValStr := r1.GetByName("val_l").GetAsString()
		rIDVal := r2.GetByName("id"); rValFlt := r2.GetByName("val_r").GetAsDouble()

		lIDScal := scalar.NewNullScalar(arrow.PrimitiveTypes.Int64); if !lIDVal.IsNil() { lIDScal = scalar.NewInt64Scalar(lIDVal.GetAsInt()) }
		lValScal := scalar.NewStringScalar(lValStr)
		rIDScal := scalar.NewNullScalar(arrow.PrimitiveTypes.Int64); if !rIDVal.IsNil() { rIDScal = scalar.NewInt64Scalar(rIDVal.GetAsInt()) }
		rValScal := scalar.NewFloat64Scalar(rValFlt)
		combinedStr := fmt.Sprintf("%s_%.1f", lValStr, rValFlt)
		combinedScal := scalar.NewStringScalar(combinedStr)

		rowVals := []scalar.Scalar{lIDScal, lValScal, rIDScal, rValScal, combinedScal}
		return []df.Row{arrowimpl.NewArrowRow(outJoinDfSchema, rowVals)}
	}
	joinCols := map[string]string{"id": "id"}

	// Case 1: Basic Inner Join (EquiJoin)
	joinedDf := ldf.Join(outJoinDfSchema, rdf, df.JoinEqui, joinCols, fUser);	defer joinedDf.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(4), joinedDf.Len(), "EquiJoin: Length check")
	expectedData := [][]interface{}{
		{int64(2), "L2", int64(2), 20.2, "L2_20.2"},
		{int64(3), "L3", int64(3), 30.3, "L3_30.3"},
		{int64(3), "L3", int64(3), 30.33, "L3_30.3"}, // Note: fUser uses "%.1f" for float in combined string
		{nilPlaceholder, "L5_nil_id", nilPlaceholder, 99.9, "L5_nil_id_99.9"},
	}
	actualData := dfToSliceOfInterfaceSlices(joinedDf)
	sortSliceOfInterfaceSlices(expectedData); sortSliceOfInterfaceSlices(actualData)
	assert.Equal(t, expectedData, actualData, "EquiJoin: Data check")

	// Case 2: No matches
	noMatchRdfBuilder := array.NewRecordBuilder(mem, rSchema); defer noMatchRdfBuilder.Release()
	noMatchRdfBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{101, 102}, nil)
	noMatchRdfBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{1.0, 2.0}, nil)
	noMatchRec := noMatchRdfBuilder.NewRecord(); defer noMatchRec.Release()
	noMatchRdf := arrowimpl.NewArrowDataFrame("no_match_rdf", noMatchRec, rdfSchema);	defer noMatchRdf.(*arrowimpl.ArrowDataFrame).Release()
	joinedNoMatch := ldf.Join(outJoinDfSchema, noMatchRdf, df.JoinEqui, joinCols, fUser);	defer joinedNoMatch.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), joinedNoMatch.Len(), "EquiJoin with no matches")

	// Case 3: Empty left DataFrame
	emptyLRec := array.NewRecord(lSchema, nil, 0); defer emptyLRec.Release()
	emptyLdf := arrowimpl.NewArrowDataFrame("empty_left", emptyLRec, ldfSchema);	defer emptyLdf.(*arrowimpl.ArrowDataFrame).Release()
	joinedEmptyLeft := emptyLdf.Join(outJoinDfSchema, rdf, df.JoinEqui, joinCols, fUser);	defer joinedEmptyLeft.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(0), joinedEmptyLeft.Len(), "EquiJoin with empty left DF")

	// Case 5: fUser returns multiple rows
	fUserMulti := func(r1, r2 df.Row) []df.Row { return append(fUser(r1,r2), fUser(r1,r2)...) }
	joinedMulti := ldf.Join(outJoinDfSchema, rdf, df.JoinEqui, joinCols, fUserMulti);	defer joinedMulti.(*arrowimpl.ArrowDataFrame).Release()
	assert.Equal(t, int64(4*2), joinedMulti.Len(), "EquiJoin with fUser returning multiple rows")

	// Case 6: Panic conditions
	assert.PanicsWithValue(t, "JoinEqui requires join columns.", func() { ldf.Join(outJoinDfSchema, rdf, df.JoinEqui, map[string]string{}, fUser) })
	assert.PanicsWithValue(t, "Join: only JoinEqui (and basic CrossJoin kernel) supported. Got LeftOuterJoin", func() { ldf.Join(outJoinDfSchema, rdf, df.JoinLeft, joinCols, fUser) })
	assert.PanicsWithValue(t, "Join: user function fUser cannot be nil in this implementation", func() { ldf.Join(outJoinDfSchema, rdf, df.JoinEqui, joinCols, nil) })
}

func TestArrowDataFrame_Join_CrossJoin_Partial(t *testing.T) {
	mem := memory.NewGoAllocator()
	lSchema := arrow.NewSchema([]arrow.Field{{Name: "L1", Type: arrow.PrimitiveTypes.Int64}}, nil)
	ldfSchema := arrowimpl.NewArrowDataFrameSchema(lSchema).(*arrowimpl.ArrowDataFrameSchema)
	lrb := array.NewRecordBuilder(mem, lSchema); defer lrb.Release()
	lrb.Field(0).(*array.Int64Builder).AppendValues([]int64{1,2}, nil)
	lRec := lrb.NewRecord(); defer lRec.Release()
	ldf := arrowimpl.NewArrowDataFrame("left_cj", lRec, ldfSchema);	defer ldf.(*arrowimpl.ArrowDataFrame).Release()

	rSchema := arrow.NewSchema([]arrow.Field{{Name: "R1", Type: arrow.BinaryTypes.String}}, nil)
	rdfSchema := arrowimpl.NewArrowDataFrameSchema(rSchema).(*arrowimpl.ArrowDataFrameSchema)
	rrb := array.NewRecordBuilder(mem, rSchema); defer rrb.Release()
	rrb.Field(0).(*array.StringBuilder).AppendValues([]string{"a","b"}, nil)
	rRec := rrb.NewRecord(); defer rRec.Release()
	rdf := arrowimpl.NewArrowDataFrame("right_cj", rRec, rdfSchema);	defer rdf.(*arrowimpl.ArrowDataFrame).Release()

	outCrossSchemaArrow := arrow.NewSchema( []arrow.Field{{Name: "L_val", Type: arrow.PrimitiveTypes.Int64}, {Name: "R_val", Type: arrow.BinaryTypes.String}}, nil )
	outCrossDfSchema := arrowimpl.NewArrowDataFrameSchema(outCrossSchemaArrow).(*arrowimpl.ArrowDataFrameSchema)
	fUserCross := func(r1, r2 df.Row) []df.Row { return []df.Row{} }

	assert.PanicsWithValue(t, "Join: CrossJoin with fUser post-processing not fully implemented after Arrow kernel.", func() {
		ldf.Join(outCrossDfSchema, rdf, df.JoinCross, nil, fUserCross)
	}, "CrossJoin path expected to panic due to incomplete fUser adaptation")
}

// TODO: Add tests for df.go (This was the original comment in the file)
