package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/log"

	// initialize sqlite
	_ "github.com/mattn/go-sqlite3"
)

// ConfigEngineStorage -  default storage, defaults to memory
const ConfigEngineStorage = "engine.storage"

type sqlliteQueryEngine struct {
	db     *sql.DB
	dbFile *os.File
}

func getSqliteType(c df.DataFrameFormat) string {
	if c.Name() == "string" {
		return "text"
	}
	return c.Name()
}

func (t *sqlliteQueryEngine) createTable(tableName string, cols []df.Column) (err error) {
	sqlStmt := `create table "%s" (%s);`
	columnStr := ""
	for _, col := range cols {
		columnStr = columnStr + " \"" + col.Name + "\" " + getSqliteType(col.Format) + " ,"
	}

	sqlStmt = fmt.Sprintf(sqlStmt, tableName, columnStr[0:len(columnStr)-1])
	_, err = t.db.Exec(sqlStmt)

	return err
}

func (t *sqlliteQueryEngine) insertData(dataFrame df.DataFrame) (err error) {

	cols, err := dataFrame.Schema()
	if err != nil {
		return err
	}
	data, err := dataFrame.Data()
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return
	}

	valueStrings := make([]string, 0, len(data))
	valueArgs := make([]interface{}, 0, len(data)*len(cols))

	colString := ""
	quesString := ""

	for _, col := range cols {
		colString = colString + "\"" + col.Name + "\","
		quesString = quesString + "?,"
	}

	colString = colString[0 : len(colString)-1]
	quesString = quesString[0 : len(quesString)-1]

	for _, d := range data {
		valueStrings = append(valueStrings, "("+quesString+")")
		valueArgs = append(valueArgs, d...)
	}
	stmt := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES %s", dataFrame.Name(), colString, strings.Join(valueStrings, ","))

	_, err = t.db.Exec(stmt, valueArgs...)

	return err
}

func (t *sqlliteQueryEngine) registerDataFrame(dataFrame df.DataFrame) error {

	cols, err := dataFrame.Schema()
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return errors.New("Columns are empty for source - " + dataFrame.Name())
	}

	err = t.createTable(dataFrame.Name(), cols)
	if err != nil {
		return err
	}

	err = t.insertData(dataFrame)
	return err
}

func (t *sqlliteQueryEngine) Query(query string, data []df.DataFrame) (result df.DataFrame, err error) {
	for _, r := range data {
		err = t.registerDataFrame(r)
		if err != nil {
			return result, err
		}
	}

	rows, err := t.db.Query(query)
	if err != nil {
		return
	}
	defer rows.Close()

	sqlCols, err := rows.Columns()
	if err != nil {
		return
	}
	sqlColTypes, err := rows.ColumnTypes()
	if err != nil {
		return
	}

	cols := make([]df.Column, len(sqlCols))

	for i, c := range sqlCols {
		dfFormat, err := df.GetFormat(sqlColTypes[i].DatabaseTypeName())
		if err != nil {
			log.Debug("sql format error for - %s, %s, %s", c, sqlColTypes[i].DatabaseTypeName(), err)
			dfFormat, err = df.GetFormat("string")
		}
		cols[i] = df.Column{Name: c, Format: dfFormat}
	}

	dataRows := make([][]interface{}, 0)

	for rows.Next() {
		dataRowPtrs := make([]interface{}, len(sqlCols))
		for i := range dataRowPtrs {
			var dataCell interface{}
			dataRowPtrs[i] = &dataCell
		}
		err = rows.Scan(dataRowPtrs...)
		if err != nil {
			return
		}

		dataRow := make([]interface{}, len(sqlCols))
		for i, cellPtr := range dataRowPtrs {
			dataRow[i], err = cols[i].Format.Convert(*(cellPtr.(*interface{})))
			if err != nil {
				return result, err
			}
		}

		dataRows = append(dataRows, dataRow)
	}

	err = rows.Err()
	if err != nil {
		return
	}

	inMempryDf := df.NewInmemoryDataframe(cols, dataRows)
	result = &inMempryDf
	return
}

func (t *sqlliteQueryEngine) Close() {
	t.db.Close()
	t.dbFile.Close()
	if t.dbFile != nil {
		fileName := t.dbFile.Name()
		os.Remove(fileName)
	}
}

func newSQLiteEngine(config map[string]string) (engine sqlliteQueryEngine, err error) {
	var db *sql.DB
	format, ok := config[ConfigEngineStorage]
	if !ok {
		format = "memory"
	}

	if format == "memory" {
		db, err = sql.Open("sqlite3", ":memory:")
		return sqlliteQueryEngine{db: db}, nil
	}
	dataFile, err := ioutil.TempFile("", "pq.*.sq")
	if err != nil {
		return engine, err
	}
	db, err = sql.Open("sqlite3", dataFile.Name())
	if err != nil {
		return engine, err
	}
	return sqlliteQueryEngine{db: db, dbFile: dataFile}, nil
}
