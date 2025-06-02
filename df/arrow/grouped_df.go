//go:build arrow
package arrow

import (
	"context"
	"fmt"
	// "reflect"
	// "time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	// "github.com/apache/arrow/go/v14/arrow/builder" // Not directly used in these specific methods
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	// "github.com/apache/arrow/go/v14/arrow/scalar" // Not directly used in these specific methods
	"github.com/blue4209211/pq/df"
)

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

	keyRecReader := array.NewTableReader(agdf.uniqueKeysTable, -1)
	defer keyRecReader.Release()

	dfRows := make([]df.Row, 0, agdf.uniqueKeysTable.NumRows())

	for keyRecReader.Next() {
		rec := keyRecReader.Record()
		for i := int64(0); i < rec.NumRows(); i++ {
			keyRow, err := NewArrowRowFromRecord(keyRowSchema, rec, int(i))
			if err != nil {
				rec.Release()
				panic(fmt.Sprintf("GetKeys: error creating df.Row from key record: %v", err))
			}
			dfRows = append(dfRows, keyRow)
		}
		rec.Release()
	}
	if keyRecReader.Err() != nil {
		panic(fmt.Sprintf("GetKeys: error reading uniqueKeysTable: %v", keyRecReader.Err()))
	}

	return dfRows
}

func (agdf *arrowGroupedDataFrame) Len() int64 {
	if agdf.uniqueKeysTable == nil {
		return 0
	}
	return agdf.uniqueKeysTable.NumRows()
}

func (agdf *arrowGroupedDataFrame) Get(keyRow df.Row) df.DataFrame {
	if agdf.originalRecord == nil {
		panic("Get called on GroupedDataFrame with nil originalRecord")
	}
	if keyRow == nil || keyRow.Len() == 0 {
		panic("Get: keyRow cannot be nil or empty")
	}
	if keyRow.Len() != len(agdf.groupingColNames) {
		panic(fmt.Sprintf("Get: keyRow has %d values, expected %d (number of grouping keys)", keyRow.Len(), len(agdf.groupingColNames)))
	}

	ctx := compute.WithAllocator(context.Background(), agdf.mem)
	var combinedMaskDatum arrow.Datum
	// Ensure combinedMaskDatum is released if it's not nil at the end or on early exit/panic path
	// However, its lifecycle is managed by being replaced or released after Filter.

	for i, groupColName := range agdf.groupingColNames {
		keyVal := keyRow.Get(i)

		originalColIdx := agdf.originalSchema.GetIndexByName(groupColName)
		if originalColIdx == -1 {
			if combinedMaskDatum != nil { combinedMaskDatum.Release() }
			panic(fmt.Sprintf("Get: grouping column '%s' not found in original schema", groupColName))
		}
		originalColumnArray := agdf.originalRecord.Column(originalColIdx)

		keyScalar, err := dfValueToArrowScalar(keyVal, originalColumnArray.DataType(), agdf.mem)
		if err != nil {
			if combinedMaskDatum != nil { combinedMaskDatum.Release() }
			panic(fmt.Sprintf("Get: error converting keyRow value for column '%s' to Arrow scalar: %v", groupColName, err))
		}

		releasableKeyScalar, needsKeyScalarRelease := keyScalar.(interface{ Release() })

		colDatum := arrow.NewArrayDatum(originalColumnArray)
		scalarDatum := arrow.NewScalarDatum(keyScalar)
		currentMaskDatum, err := compute.Compare(ctx, colDatum, scalarDatum, compute.Equal)

		if needsKeyScalarRelease { releasableKeyScalar.Release() } // Release casted scalar after use

		if err != nil {
			if combinedMaskDatum != nil { combinedMaskDatum.Release() }
			panic(fmt.Sprintf("Get: error comparing column '%s' with key value: %v", groupColName, err))
		}

		if combinedMaskDatum == nil {
			combinedMaskDatum = currentMaskDatum
		} else {
			prevCombinedMask := combinedMaskDatum
			newCombinedMaskDatum, errAnd := compute.And(ctx, prevCombinedMask, currentMaskDatum)
			currentMaskDatum.Release()
			prevCombinedMask.Release()
			if errAnd != nil {
				panic(fmt.Sprintf("Get: error ANDing masks for column '%s': %v", groupColName, errAnd))
			}
			combinedMaskDatum = newCombinedMaskDatum
		}
	}

	if combinedMaskDatum == nil {
		emptyRec := array.NewRecord(agdf.originalSchema.schema, nil, 0); defer emptyRec.Release()
		return NewArrowDataFrameWithAllocator(agdf.originalSchema.Name()+"_group_emptykey", emptyRec, agdf.originalSchema, agdf.mem)
	}

	groupRecordDatum, err := compute.Filter(ctx, arrow.NewRecordDatum(agdf.originalRecord), combinedMaskDatum, compute.FilterOptions{NullSelectionBehavior: compute.Drop})
	combinedMaskDatum.Release()
	if err != nil {
		panic(fmt.Sprintf("Get: error filtering original record for group: %v", err))
	}
	defer groupRecordDatum.Release()

	groupRecordResult, ok := groupRecordDatum.(*arrow.RecordDatum)
    if !ok || groupRecordResult == nil { panic("Get: Filter did not return a valid RecordDatum") }
    groupRecord := groupRecordResult.Value().(arrow.Record)

	return NewArrowDataFrameWithAllocator(agdf.originalSchema.Name()+"_group", groupRecord, agdf.originalSchema, agdf.mem)
}

func (agdf *arrowGroupedDataFrame) ForEach(f func(key df.Row, groupDf df.DataFrame)) {
	if f == nil {
		panic("ForEach: function f cannot be nil")
	}
	keys := agdf.GetKeys()
	for _, keyRow := range keys {
		groupDataFrame := agdf.Get(keyRow)
		arrowGroupDf, ok := groupDataFrame.(*arrowDataFrame)
		if !ok && groupDataFrame != nil {
			panic(fmt.Sprintf("ForEach: agdf.Get() returned unexpected DataFrame type: %T", groupDataFrame))
		}
		f(keyRow, groupDataFrame)
		if arrowGroupDf != nil { arrowGroupDf.Release() }
	}
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
