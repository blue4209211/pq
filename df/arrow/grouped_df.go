//go:build arrow
package arrow

import (
	"context"
	"fmt"
	// "reflect"
	// "time"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	// "github.com/apache/arrow/go/v14/arrow/builder"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	// "github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/blue4209211/pq/df"
)

type arrowGroupedDataFrame struct {
	originalRecord    arrow.Record // This is the full record from which groups are derived.
	originalSchema    *arrowDataFrameSchema
	groupingColNames  []string
	uniqueKeysTable   arrow.Table // Table containing unique key combinations.
	mem               memory.Allocator
}

var _ df.GroupedDataFrame = (*arrowGroupedDataFrame)(nil)

func (agdf *arrowGroupedDataFrame) GetGroupColumns() []string {
	names := make([]string, len(agdf.groupingColNames))
	copy(names, agdf.groupingColNames)
	return names
}

func (agdf *arrowGroupedDataFrame) GetKeys() []df.Row {
	if agdf.uniqueKeysTable == nil || agdf.uniqueKeysTable.NumRows() == 0 {
		return []df.Row{}
	}
	keyRowSchema := NewArrowDataFrameSchema(agdf.uniqueKeysTable.Schema()).(*arrowDataFrameSchema)
	keyRecReader := array.NewTableReader(agdf.uniqueKeysTable, -1);	defer keyRecReader.Release()
	dfRows := make([]df.Row, 0, agdf.uniqueKeysTable.NumRows())
	for keyRecReader.Next() {
		rec := keyRecReader.Record();
		for i := int64(0); i < rec.NumRows(); i++ {
			keyRow, err := NewArrowRowFromRecord(keyRowSchema, rec, int(i))
			if err != nil { panic(fmt.Sprintf("GetKeys: error creating df.Row from key record: %v", err)) }
			dfRows = append(dfRows, keyRow)
		}
	}
	if keyRecReader.Err() != nil { panic(fmt.Sprintf("GetKeys: error reading uniqueKeysTable: %v", keyRecReader.Err())) }
	return dfRows
}

func (agdf *arrowGroupedDataFrame) Len() int64 {
	if agdf.uniqueKeysTable == nil { return 0 }
	return agdf.uniqueKeysTable.NumRows()
}

func (agdf *arrowGroupedDataFrame) Get(keyRow df.Row) df.DataFrame {
	if agdf.originalRecord == nil { panic("Get called on GroupedDataFrame with nil originalRecord") }
	if keyRow == nil || keyRow.Len() == 0 { panic("Get: keyRow cannot be nil or empty") }
	if keyRow.Len() != len(agdf.groupingColNames) {
		panic(fmt.Sprintf("Get: keyRow has %d values, expected %d (grouping keys)", keyRow.Len(), len(agdf.groupingColNames)))
	}

	ctx := compute.WithAllocator(context.Background(), agdf.mem)
	var combinedMaskDatum arrow.Datum

	for i, groupColName := range agdf.groupingColNames {
		keyVal := keyRow.Get(i)
		originalColIdx := agdf.originalSchema.GetIndexByName(groupColName)
		if originalColIdx == -1 { if combinedMaskDatum != nil { combinedMaskDatum.Release() }; panic(fmt.Sprintf("Get: grouping column '%s' not found in original schema", groupColName)) }
		originalColumnArray := agdf.originalRecord.Column(originalColIdx)

		keyScalar, err := dfValueToArrowScalar(keyVal, originalColumnArray.DataType(), agdf.mem)
		if err != nil { if combinedMaskDatum != nil { combinedMaskDatum.Release() }; panic(fmt.Sprintf("Get: error converting keyRow value for col '%s' to Arrow scalar: %v", groupColName, err)) }

		releasableKeyScalar, needsKeyScalarRelease := keyScalar.(interface{ Release() })

		colDatum := arrow.NewArrayDatum(originalColumnArray)
		scalarDatum := arrow.NewScalarDatum(keyScalar)
		currentMaskDatum, err := compute.Compare(ctx, colDatum, scalarDatum, compute.Equal)

		if needsKeyScalarRelease { releasableKeyScalar.Release() }
		if err != nil { if combinedMaskDatum != nil { combinedMaskDatum.Release() }; panic(fmt.Sprintf("Get: error comparing column '%s' with key value: %v", groupColName, err)) }

		if combinedMaskDatum == nil {
			combinedMaskDatum = currentMaskDatum
		} else {
			prevCombinedMask := combinedMaskDatum
			newCombinedMaskDatum, errAnd := compute.And(ctx, prevCombinedMask, currentMaskDatum)
			currentMaskDatum.Release()
			prevCombinedMask.Release()
			if errAnd != nil { panic(fmt.Sprintf("Get: error ANDing masks for column '%s': %v", groupColName, errAnd)) }
			combinedMaskDatum = newCombinedMaskDatum
		}
	}

	if combinedMaskDatum == nil { // Should not happen if groupingColNames is not empty
		emptyRec := array.NewRecord(agdf.originalSchema.schema, nil, 0); defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(agdf.originalSchema.Name()+"_group_emptykey", emptyRec, agdf.originalSchema, adf.mem)
	}

	groupRecordDatum, err := compute.Filter(ctx, arrow.NewRecordDatum(agdf.originalRecord), combinedMaskDatum, compute.FilterOptions{NullSelectionBehavior: compute.Drop})
	combinedMaskDatum.Release()
	if err != nil { panic(fmt.Sprintf("Get: error filtering original record for group: %v", err)) }
	defer groupRecordDatum.Release()

	groupRecordResult, ok := groupRecordDatum.(*arrow.RecordDatum)
    if !ok || groupRecordResult == nil { panic("Get: Filter did not return a valid RecordDatum") }
    groupRecordVal := groupRecordResult.Value()
	if groupRecordVal == nil { panic("Get: Filter RecordDatum Value is nil") }
	groupRecord := groupRecordVal.(arrow.Record)

	return NewArrowDataFrameWithAllocator(agdf.originalSchema.Name()+"_group", groupRecord, agdf.originalSchema, adf.mem)
}

func (agdf *arrowGroupedDataFrame) ForEach(f func(key df.Row, groupDf df.DataFrame)) {
	if f == nil { panic("ForEach: function f cannot be nil") }
	keys := agdf.GetKeys()
	for _, keyRow := range keys {
		groupDataFrame := agdf.Get(keyRow)
		// No need to cast to arrowDataFrame for Release, df.Releaser is enough
		f(keyRow, groupDataFrame)
		if releasable, ok := groupDataFrame.(df.Releaser); ok { releasable.Release() }
	}
}

func (agdf *arrowGroupedDataFrame) Agg(configs ...df.AggregationConfig) df.DataFrame {
	if agdf.originalRecord == nil { panic("Agg called on GroupedDataFrame with nil originalRecord") }
	if len(configs) == 0 { // Return distinct keys if no aggregations specified
		if agdf.uniqueKeysTable.NumRows() == 0 {
			emptyKeySchema := agdf.uniqueKeysTable.Schema()
			emptyKeyRecord := array.NewRecord(emptyKeySchema, nil, 0); defer emptyKeyRecord.Release()
			return NewArrowDataFrameWithAllocator("agg_keys_empty", emptyKeyRecord, NewArrowDataFrameSchema(emptyKeySchema).(*arrowDataFrameSchema), agdf.mem)
		}
		// Convert Table to Record(s) then to DataFrame
		// This path might be simplified if uniqueKeysTable can be directly wrapped.
		// For now, ensure it becomes a single record for NewArrowDataFrameWithAllocator.
		tblReader := array.NewTableReader(agdf.uniqueKeysTable, -1); defer tblReader.Release()
		var records []arrow.Record
		for tblReader.Next() { rec := tblReader.Record(); rec.Retain(); records = append(records, rec) }
		if tblReader.Err() != nil { panic(fmt.Sprintf("Agg: error reading uniqueKeysTable: %v", tblReader.Err()))}
		if len(records) == 0 { /* Should be caught by NumRows == 0 */ }

		var keysRecord arrow.Record
		if len(records) == 1 { keysRecord = records[0]
		} else { var errConcat error; keysRecord, errConcat = array.ConcatenateRecords(agdf.uniqueKeysTable.Schema(), records, agdf.mem); if errConcat != nil { panic(errConcat)}; for _, r := range records { r.Release() } }
		defer keysRecord.Release()
		return NewArrowDataFrameWithAllocator("agg_keys", keysRecord, NewArrowDataFrameSchema(agdf.uniqueKeysTable.Schema()).(*arrowDataFrameSchema), agdf.mem)
	}

	ctx := compute.WithAllocator(context.Background(), agdf.mem)
	groupKeyRefs := make([]arrow.FieldRef, len(agdf.groupingColNames))
	for i, name := range agdf.groupingColNames { ref, err := arrow.FieldRefFromPath(name); if err != nil { panic(err) }; groupKeyRefs[i] = ref }

	computeAggs := make([]compute.Aggregate, len(configs))
	for i, cfg := range configs {
		var inputRef *arrow.FieldRef; var aggOpts compute.FunctionOptions = nil
		if cfg.InputCol != "" { ref, err := arrow.FieldRefFromPath(cfg.InputCol); if err != nil { panic(err) }; inputRef = &ref
		} else if strings.ToLower(cfg.Func) == "count" { aggOpts = &compute.CountOptions{Mode: compute.CountAll} }
		arrowFuncName := strings.ToLower(cfg.Func); if arrowFuncName == "avg" { arrowFuncName = "mean" }
		computeAggs[i] = compute.Aggregate{Name: arrowFuncName, Input: inputRef, Output: cfg.OutputColName, Options: aggOpts }
	}

	inputDatum := arrow.NewRecordDatum(agdf.originalRecord); defer inputDatum.Release()
	aggResultDatum, err := compute.GroupBy(ctx, inputDatum, groupKeyRefs, computeAggs)
	if err != nil { panic(fmt.Sprintf("Agg: compute.GroupBy failed: %v", err)) }; defer aggResultDatum.Release()

	aggResultVal := aggResultDatum.Value()
	if aggResultVal == nil { panic("Agg: compute.GroupBy result datum value is nil") }
	resultRecord, ok := aggResultVal.(arrow.Record)
	if !ok { panic("Agg: compute.GroupBy did not return a Record as expected") }

	resultDfSchema := NewArrowDataFrameSchema(resultRecord.Schema()).(*arrowDataFrameSchema)
	return NewArrowDataFrameWithAllocator(agdf.name+"_agg", resultRecord, resultDfSchema, adf.mem)
}

func (agdf *arrowGroupedDataFrame) Map(f func(key df.Row, groupDf df.DataFrame) df.DataFrame) df.GroupedDataFrame {
	if f == nil { panic("Map: map function f cannot be nil") }
	if agdf.uniqueKeysTable == nil || agdf.uniqueKeysTable.NumRows() == 0 { return agdf }

	keys := agdf.GetKeys()
	if len(keys) == 0 { return agdf }

	mappedGroupDataFrames := make([]df.DataFrame, 0, len(keys))
	defer func() { for _, mappedDf := range mappedGroupDataFrames { if r, ok := mappedDf.(df.Releaser); ok { r.Release() } } }()

	var firstNonEmptyResultArrowSchema *arrow.Schema = nil

	for _, keyRow := range keys {
		groupDf := agdf.Get(keyRow) // This is an arrowDataFrame
		transformedDf := f(keyRow, groupDf)
		if releasable, ok := groupDf.(df.Releaser); ok { releasable.Release() }

		if transformedDf == nil { panic(fmt.Sprintf("Map: function f returned a nil DataFrame for key %v", keyRow)) }

		arrowTransformedDf, ok := transformedDf.(*arrowDataFrame)
		if !ok { if r, ok := transformedDf.(df.Releaser); ok { r.Release() }; panic(fmt.Sprintf("Map: function f must return an *arrowDataFrame, got %T for key %v", transformedDf, keyRow)) }

		mappedGroupDataFrames = append(mappedGroupDataFrames, arrowTransformedDf) // Stays in scope, released by defer

		if firstNonEmptyResultArrowSchema == nil && arrowTransformedDf.record != nil && arrowTransformedDf.record.NumCols() > 0 {
			// Use a copy of the schema, not a pointer to a potentially changing one
			s := arrowTransformedDf.record.Schema()
			firstNonEmptyResultArrowSchema = &s
		}
	}

	if len(mappedGroupDataFrames) == 0 { // Should not happen if keys is not empty
		return agdf // Or an empty grouped DF
	}

	// Determine the schema for concatenation. If all results were empty (0 rows but with schema),
	// use the schema of the first result. If all results were truly empty (0 cols), schema is empty.
	var concatSchema *arrow.Schema
	if firstNonEmptyResultArrowSchema != nil {
		concatSchema = firstNonEmptyResultArrowSchema
	} else if mappedGroupDataFrames[0].(*arrowDataFrame).schema != nil && mappedGroupDataFrames[0].(*arrowDataFrame).schema.schema != nil {
		// All groups might have mapped to DataFrames with 0 rows but a valid schema.
		concatSchema = mappedGroupDataFrames[0].(*arrowDataFrame).schema.schema
	} else {
		// Truly empty results, create an empty schema
		s := arrow.NewSchema([]arrow.Field{},nil)
		concatSchema = &s
	}


	recordsToConcat := make([]arrow.Record, 0, len(mappedGroupDataFrames))
	for i, dfInstance := range mappedGroupDataFrames {
		adf := dfInstance.(*arrowDataFrame) // Already type-checked
		if adf.record == nil || adf.record.NumRows() == 0 { continue } // Skip empty records

		if !adf.record.Schema().Equal(*concatSchema) {
			panic(fmt.Sprintf("Map: schema mismatch for concatenation. Group key (index %d of %d keys) resulted in schema\n%s\nExpected schema (from first non-empty result)\n%s",
				i, len(keys), adf.record.Schema().String(), concatSchema.String()))
		}
		adf.record.Retain(); recordsToConcat = append(recordsToConcat, adf.record)
	}
	defer func() { for _, rec := range recordsToConcat { rec.Release() } }()

	var concatenatedRecord arrow.Record
	if len(recordsToConcat) == 0 {
		concatenatedRecord = array.NewRecord(concatSchema, nil, 0)
	} else {
		var err error
		concatenatedRecord, err = array.ConcatenateRecords(*concatSchema, recordsToConcat, agdf.mem)
		if err != nil { panic(fmt.Sprintf("Map: failed to concatenate records: %v", err)) }
	}
	defer concatenatedRecord.Release()

	concatenatedDfSchema := NewArrowDataFrameSchema(concatenatedRecord.Schema()).(*arrowDataFrameSchema)
	concatenatedBaseDf := NewArrowDataFrameWithAllocator(agdf.name+"_map_result", concatenatedRecord, concatenatedDfSchema, agdf.mem)
	defer concatenatedBaseDf.(df.Releaser).Release()

	// Re-group using original grouping column names.
	// This assumes the map function `f` preserves these columns with compatible types.
	for _, groupColName := range agdf.groupingColNames {
		if concatenatedBaseDf.Schema().GetIndexByName(groupColName) == -1 {
			panic(fmt.Sprintf("Map: grouping column '%s' is missing from the DataFrame returned by the map function. The map function must preserve grouping columns for re-grouping.", groupColName))
		}
	}

	finalGroupedDf := concatenatedBaseDf.GroupBy(agdf.groupingColNames...)
	return finalGroupedDf
}

func (agdf *arrowGroupedDataFrame) Where(f func(key df.Row, groupDf df.DataFrame) bool) df.GroupedDataFrame {
	if f == nil { panic("Where: filter function f cannot be nil") }
	if agdf.uniqueKeysTable == nil || agdf.uniqueKeysTable.NumRows() == 0 { return agdf }

	ctx := compute.WithAllocator(context.Background(), agdf.mem)
	keptKeyIndices := make([]int64, 0)

	keyTblReader := array.NewTableReader(agdf.uniqueKeysTable, -1); defer keyTblReader.Release()
	keyRowSchema := NewArrowDataFrameSchema(agdf.uniqueKeysTable.Schema()).(*arrowDataFrameSchema)
	currentKeyIndexOffset := int64(0)

	for keyTblReader.Next() {
		keyRecord := keyTblReader.Record()
		for i := 0; i < int(keyRecord.NumRows()); i++ {
			keyRow, err := NewArrowRowFromRecord(keyRowSchema, keyRecord, i)
			if err != nil { panic(fmt.Sprintf("Where: error creating df.Row from key record: %v", err)) }
			groupDf := agdf.Get(keyRow)
			if f(keyRow, groupDf) { keptKeyIndices = append(keptKeyIndices, currentKeyIndexOffset + int64(i)) }
			if releasable, ok := groupDf.(df.Releaser); ok { releasable.Release() }
		}
		currentKeyIndexOffset += keyRecord.NumRows()
	}
	if keyTblReader.Err() != nil { panic(fmt.Sprintf("Where: error reading uniqueKeysTable: %v", keyTblReader.Err())) }

	if len(keptKeyIndices) == 0 {
		emptyKeysTable, _ := array.NewTableFromRecords(agdf.uniqueKeysTable.Schema(), []arrow.Record{}); defer emptyKeysTable.Release()
		agdf.originalRecord.Retain()
		return &arrowGroupedDataFrame{
			originalRecord:   agdf.originalRecord, originalSchema:   agdf.originalSchema,
			groupingColNames: agdf.groupingColNames, uniqueKeysTable:  emptyKeysTable,
			mem:              agdf.mem,
		}
	}

	indicesBuilder := array.NewInt64Builder(agdf.mem); defer indicesBuilder.Release()
	indicesBuilder.AppendValues(keptKeyIndices, nil)
	indicesArr := indicesBuilder.NewArray(); defer indicesArr.Release()

	filteredKeysDatum, err := compute.TakeTable(ctx, agdf.uniqueKeysTable, arrow.NewArrayDatum(indicesArr), compute.TakeOptions{})
	if err != nil { panic(fmt.Sprintf("Where: failed to Take from uniqueKeysTable: %v", err)) }
	defer filteredKeysDatum.Release()

	newUniqueKeysTable, ok := filteredKeysDatum.Value().(arrow.Table);
	if !ok { panic("Where: TakeTable did not return arrow.Table") }
	newUniqueKeysTable.Retain()

	agdf.originalRecord.Retain();
	return &arrowGroupedDataFrame{
		originalRecord:    agdf.originalRecord, originalSchema:    agdf.originalSchema,
		groupingColNames:  agdf.groupingColNames, uniqueKeysTable:   newUniqueKeysTable,
		mem:               agdf.mem,
	}
}

func (agdf *arrowGroupedDataFrame) Release() {
	if agdf.originalRecord != nil { agdf.originalRecord.Release(); agdf.originalRecord = nil }
	if agdf.uniqueKeysTable != nil { agdf.uniqueKeysTable.Release(); agdf.uniqueKeysTable = nil }
}
