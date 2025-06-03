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

// AggregationConfig defines how a single aggregation should be performed.
type AggregationConfig struct {
	Func          string // e.g., "sum", "mean", "count", "min", "max"
	InputCol      string // Column to aggregate. Empty for count_all behavior.
	OutputColName string // Name of the resulting aggregated column.
}

type arrowGroupedDataFrame struct {
	originalRecord    arrow.Record
	originalSchema    *arrowDataFrameSchema
	groupingColNames  []string
	uniqueKeysTable   arrow.Table
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
		rec := keyRecReader.Record(); // This record is managed by TableReader for current iteration
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

	if combinedMaskDatum == nil {
		emptyRec := array.NewRecord(agdf.originalSchema.schema, nil, 0); defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(agdf.originalSchema.Name()+"_group_emptykey", emptyRec, agdf.originalSchema, agdf.mem)
	}

	groupRecordDatum, err := compute.Filter(ctx, arrow.NewRecordDatum(agdf.originalRecord), combinedMaskDatum, compute.FilterOptions{NullSelectionBehavior: compute.Drop})
	combinedMaskDatum.Release()
	if err != nil { panic(fmt.Sprintf("Get: error filtering original record for group: %v", err)) }
	defer groupRecordDatum.Release()

	groupRecordResult, ok := groupRecordDatum.(*arrow.RecordDatum)
    if !ok || groupRecordResult == nil { panic("Get: Filter did not return a valid RecordDatum") }
    groupRecord := groupRecordResult.Value().(arrow.Record)

	return NewArrowDataFrameWithAllocator(agdf.originalSchema.Name()+"_group", groupRecord, agdf.originalSchema, agdf.mem)
}

func (agdf *arrowGroupedDataFrame) ForEach(f func(key df.Row, groupDf df.DataFrame)) {
	if f == nil { panic("ForEach: function f cannot be nil") }
	keys := agdf.GetKeys()
	for _, keyRow := range keys {
		groupDataFrame := agdf.Get(keyRow)
		arrowGroupDf, ok := groupDataFrame.(*arrowDataFrame)
		if !ok && groupDataFrame != nil { panic(fmt.Sprintf("ForEach: agdf.Get() returned unexpected DataFrame type: %T", groupDataFrame)) }
		f(keyRow, groupDataFrame)
		if arrowGroupDf != nil { arrowGroupDf.Release() }
	}
}

func (agdf *arrowGroupedDataFrame) Agg(configs ...AggregationConfig) df.DataFrame {
	if agdf.originalRecord == nil { panic("Agg called on GroupedDataFrame with nil originalRecord") }

	if len(configs) == 0 {
		if agdf.uniqueKeysTable.NumRows() == 0 {
			emptyKeySchema := agdf.uniqueKeysTable.Schema()
			emptyKeyRecord := array.NewRecord(emptyKeySchema, nil, 0); defer emptyKeyRecord.Release()
			return NewArrowDataFrameWithAllocator("agg_keys_empty", emptyKeyRecord,
				NewArrowDataFrameSchema(emptyKeySchema).(*arrowDataFrameSchema), agdf.mem)
		}
		tblReader := array.NewTableReader(agdf.uniqueKeysTable, -1); defer tblReader.Release() // Read all chunks
		var records []arrow.Record
		for tblReader.Next() {
			rec := tblReader.Record(); rec.Retain(); records = append(records, rec)
		}
		if tblReader.Err() != nil { panic(fmt.Sprintf("Agg: error reading uniqueKeysTable for key-only output: %v", tblReader.Err()))}
		if len(records) == 0 { // Should be caught by NumRows == 0, but defensive
			emptyKeySchema := agdf.uniqueKeysTable.Schema()
			emptyKeyRecord := array.NewRecord(emptyKeySchema, nil, 0); defer emptyKeyRecord.Release()
			return NewArrowDataFrameWithAllocator("agg_keys_empty", emptyKeyRecord, NewArrowDataFrameSchema(emptyKeySchema).(*arrowDataFrameSchema), agdf.mem)
		}
		// For simplicity, if multiple records (chunks) in uniqueKeysTable, concatenate them.
		// This is not ideal for very large key tables but handles chunking.
		var keysRecord arrow.Record
		if len(records) == 1 {
			keysRecord = records[0] // Already retained
		} else {
			var errConcat error
			keysRecord, errConcat = array.ConcatenateRecords(agdf.uniqueKeysTable.Schema(), records, agdf.mem)
			if errConcat != nil { panic(fmt.Sprintf("Agg: failed to concatenate key records: %v", errConcat))}
			// Release individual retained records as ConcatenateRecords makes a new one.
			for _, r := range records { r.Release() }
		}
		// keysRecord is now the one to use, NewArrowDataFrameWithAllocator will retain it.
		defer keysRecord.Release()
		return NewArrowDataFrameWithAllocator("agg_keys", keysRecord, NewArrowDataFrameSchema(agdf.uniqueKeysTable.Schema()).(*arrowDataFrameSchema), agdf.mem)
	}

	ctx := compute.WithAllocator(context.Background(), agdf.mem)
	groupKeyRefs := make([]arrow.FieldRef, len(agdf.groupingColNames))
	for i, name := range agdf.groupingColNames {
		ref, err := arrow.FieldRefFromPath(name)
		if err != nil { panic(fmt.Sprintf("Agg: invalid grouping column name '%s': %v", name, err)) }
		groupKeyRefs[i] = ref
	}

	computeAggs := make([]compute.Aggregate, len(configs))
	for i, cfg := range configs {
		var inputRef *arrow.FieldRef; var aggOpts compute.FunctionOptions = nil
		if cfg.InputCol != "" {
			ref, err := arrow.FieldRefFromPath(cfg.InputCol)
			if err != nil { panic(fmt.Sprintf("Agg: invalid input col '%s' for agg '%s': %v", cfg.InputCol, cfg.Func, err)) }
			inputRef = &ref
		} else {
			if strings.ToLower(cfg.Func) != "count" { /* Might allow other "count_all" like functions */ }
			aggOpts = &compute.CountOptions{Mode: compute.CountAll}
		}
		arrowFuncName := strings.ToLower(cfg.Func); if arrowFuncName == "avg" { arrowFuncName = "mean" }
		computeAggs[i] = compute.Aggregate{Name: arrowFuncName, Input: inputRef, Output: cfg.OutputColName, DataType: nil, Options: aggOpts }
	}

	inputDatum := arrow.NewRecordDatum(agdf.originalRecord); defer inputDatum.Release()
	aggResultDatum, err := compute.GroupBy(ctx, inputDatum, groupKeyRefs, computeAggs)
	if err != nil { panic(fmt.Sprintf("Agg: compute.GroupBy failed: %v", err)) }; defer aggResultDatum.Release()
	resultRecord, ok := aggResultDatum.(*arrow.RecordDatum).Value().(arrow.Record)
	if !ok { panic("Agg: compute.GroupBy did not return a RecordDatum as expected") }

	resultDfSchema := NewArrowDataFrameSchema(resultRecord.Schema()).(*arrowDataFrameSchema)
	aggDfName := agdf.originalSchema.Name() + "_agg"; if len(agdf.groupingColNames) > 0 { aggDfName = agdf.originalSchema.Name() + "_gb_" + strings.Join(agdf.groupingColNames, "_") }
	// NewArrowDataFrameWithAllocator will retain resultRecord
	return NewArrowDataFrameWithAllocator(aggDfName, resultRecord, resultDfSchema, agdf.mem)
}

func (agdf *arrowGroupedDataFrame) Map(f func(df.Row, df.DataFrame) df.DataFrame) df.GroupedDataFrame {
	panic("arrowGroupedDataFrame.Map not yet implemented")
}

func (agdf *arrowGroupedDataFrame) Where(f func(df.Row, df.DataFrame) bool) df.GroupedDataFrame {
	panic("arrowGroupedDataFrame.Where not yet implemented")
}

func (agdf *arrowGroupedDataFrame) Release() {
	if agdf.originalRecord != nil { agdf.originalRecord.Release(); agdf.originalRecord = nil }
	if agdf.uniqueKeysTable != nil { agdf.uniqueKeysTable.Release(); agdf.uniqueKeysTable = nil }
}
