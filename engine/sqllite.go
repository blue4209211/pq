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
	_ "github.com/mattn/go-sqlite3"
)

const ConfigEngineStorage = "engine.storage"

type SQLLiteQueryEngine struct {
	db     *sql.DB
	dbFile *os.File
}

func getSqliteType(c df.DataFrameFormat) string {
	if c.Name() == "string" {
		return "text"
	} else {
		return c.Name()
	}
}

func (self *SQLLiteQueryEngine) createTable(tableName string, cols []df.Column) (err error) {
	sqlStmt := `
				create table "%s" (%s);
			`
	columnStr := ""
	for _, col := range cols {
		columnStr = columnStr + " \"" + col.Name + "\" " + getSqliteType(col.Format) + " ,"
	}

	sqlStmt = fmt.Sprintf(sqlStmt, tableName, columnStr[0:len(columnStr)-1])
	_, err = self.db.Exec(sqlStmt)

	return err
}

func (self *SQLLiteQueryEngine) insertData(dataFrame df.DataFrame) (err error) {

	cols, err := dataFrame.Schema()
	if err != nil {
		return err
	}
	data, err := dataFrame.Data()
	if err != nil {
		return err
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

	_, err = self.db.Exec(stmt, valueArgs...)
	return err
}

func (self *SQLLiteQueryEngine) registerDataFrame(dataFrame df.DataFrame) error {

	cols, err := dataFrame.Schema()
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return errors.New("Columns are empty for source - " + dataFrame.Name())
	}

	err = self.createTable(dataFrame.Name(), cols)
	if err != nil {
		return err
	}

	err = self.insertData(dataFrame)
	return err
}

func (self *SQLLiteQueryEngine) Query(query string, data []df.DataFrame) (result df.DataFrame, err error) {
	for _, r := range data {
		err = self.registerDataFrame(r)
		if err != nil {
			return result, err
		}
	}

	rows, err := self.db.Query(query)
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

	return df.NewInmemoryDataframe(cols, dataRows), err
}

func (self *SQLLiteQueryEngine) Close() {
	self.db.Close()
	self.dbFile.Close()
	if self.dbFile != nil {
		fileName := self.dbFile.Name()
		os.Remove(fileName)
	}
}

func NewSQLiteEngine(config map[string]string) (engine SQLLiteQueryEngine, err error) {
	var db *sql.DB
	format, ok := config[ConfigEngineStorage]
	if !ok {
		format = "memory"
	}

	if format == "memory" {
		db, err = sql.Open("sqlite3", ":memory:")
		return SQLLiteQueryEngine{db: db}, nil
	} else {
		dataFile, err := ioutil.TempFile("", "pq.*.sq")
		if err != nil {
			return engine, err
		}
		db, err = sql.Open("sqlite3", dataFile.Name())
		if err != nil {
			return engine, err
		}
		return SQLLiteQueryEngine{db: db, dbFile: dataFile}, nil
	}
}
