package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/blue4209211/pq/internal/fns"
	"github.com/blue4209211/pq/internal/log"
	"github.com/mattn/go-sqlite3"
)

// ConfigEngineStorage -  default storage, defaults to memory
const ConfigEngineStorage = "engine.storage"

type sqliteQueryEngine struct {
	db     *sql.DB
	dbFile *os.File
}

func getSqliteType(c df.Format) string {
	if c.Name() == "string" {
		return "text"
	} else if c.Name() == "datetime" {
		return "text"
	}

	return c.Name()
}

func (t *sqliteQueryEngine) createTable(tableName string, cols []df.SeriesSchema) (err error) {
	sqlStmt := `create table "%s" (%s);`
	columnStr := ""
	for _, col := range cols {
		columnStr = columnStr + " \"" + col.Name + "\" " + getSqliteType(col.Format) + " ,"
	}
	sqlStmt = fmt.Sprintf(sqlStmt, tableName, columnStr[0:len(columnStr)-1])
	if err != nil {
		return err
	}
	_, err = t.db.Exec(sqlStmt)
	return err
}

func (t *sqliteQueryEngine) insertData(dataFrame df.DataFrame) (err error) {

	schema := dataFrame.Schema()
	if dataFrame.Len() == 0 {
		return
	}

	colString := ""
	quesString := ""

	for _, col := range schema.Series() {
		colString = colString + "\"" + col.Name + "\","
		quesString = quesString + "?,"
	}

	colString = colString[0 : len(colString)-1]
	quesString = quesString[0 : len(quesString)-1]

	batchSize := 1000
	totalRecords := int(dataFrame.Len())

	for i := 0; i < totalRecords; i = i + batchSize {
		valueStrings := make([]string, 0, batchSize)
		valueArgs := make([]any, 0, batchSize*schema.Len())

		for j := i; j < (i+batchSize) && j < totalRecords; j++ {
			valueStrings = append(valueStrings, "("+quesString+")")
			r := dataFrame.GetRow(int64(j))
			for k := 0; k < r.Len(); k++ {
				valueArgs = append(valueArgs, r.GetRaw(k))
			}
		}
		stmt := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES %s", dataFrame.Name(), colString, strings.Join(valueStrings, ","))

		_, err = t.db.Exec(stmt, valueArgs...)

		if err != nil {
			return err
		}
	}

	return err
}

func (t *sqliteQueryEngine) RegisterDataFrame(dataFrame df.DataFrame) error {

	schema := dataFrame.Schema()
	if schema.Len() == 0 {
		return errors.New("Columns are empty for source - " + dataFrame.Name())
	}

	log.Debug("Creating df - ", dataFrame.Name(), schema.Series())

	err := t.createTable(dataFrame.Name(), schema.Series())
	if err != nil {
		return err
	}

	log.Debug("Inserting  df - ", dataFrame.Name(), err)

	err = t.insertData(dataFrame)

	log.Debug("Registered df - ", dataFrame.Name(), err)

	return err
}

func (t *sqliteQueryEngine) Query(query string) (result df.DataFrame, err error) {
	return queryInternal(t.db, query)
}

func queryInternal(db *sql.DB, query string) (result df.DataFrame, err error) {
	rows, err := db.Query(query)
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

	cols := make([]df.SeriesSchema, len(sqlCols))

	for i, c := range sqlCols {
		dfFormat, err := df.GetFormat(sqlColTypes[i].DatabaseTypeName())
		if err != nil {
			log.Debugf("sql format error for - %s, %s, %s", c, sqlColTypes[i].DatabaseTypeName(), err)
			dfFormat, err = df.GetFormat("string")
		}
		cols[i] = df.SeriesSchema{Name: c, Format: dfFormat}
	}

	schema := df.NewSchema(cols)
	dataRows := make([]df.Row, 0, 100)

	for rows.Next() {
		dataRowPtrs := make([]any, len(sqlCols))
		for i := range dataRowPtrs {
			var dataCell any
			dataRowPtrs[i] = &dataCell
		}
		err = rows.Scan(dataRowPtrs...)
		if err != nil {
			return
		}

		dataRow := make([]df.Value, len(sqlCols))
		for i, cellPtr := range dataRowPtrs {
			v, err := cols[i].Format.Convert(*(cellPtr.(*any)))
			if err != nil {
				return result, err
			}
			dataRow[i] = inmemory.NewValue(cols[i].Format, v)
		}

		dataRows = append(dataRows, inmemory.NewRow(schema, &dataRow))
	}

	err = rows.Err()
	if err != nil {
		return
	}

	inMemoryDf := inmemory.NewDataframeFromRow(schema, &dataRows)
	result = inMemoryDf
	return
}

func (t *sqliteQueryEngine) Close() {
	t.db.Close()
	if t.dbFile != nil {
		t.dbFile.Close()
		fileName := t.dbFile.Name()
		os.Remove(fileName)
	}
}

var module pqModule = pqModule{}
var moduleRegistered bool = false

func newSQLiteEngine(config map[string]string, data []df.DataFrame) (engine queryEngine, err error) {
	var db *sql.DB
	format, ok := config[ConfigEngineStorage]
	if !ok {
		format = "memory"
	}
	if format == "memory" {
		if !moduleRegistered {
			sql.Register("sqlite3_pq", &sqlite3.SQLiteDriver{
				ConnectHook: func(conn *sqlite3.SQLiteConn) error {
					if err := conn.RegisterFunc("text_extract", fns.TextExtract, true); err != nil {
						return err
					}
					if err := conn.RegisterFunc("regexp", fns.Regexp, true); err != nil {
						return err
					}
					if err := conn.RegisterFunc("match", fns.Matches, true); err != nil {
						return err
					}
					return nil
				},
			})
			moduleRegistered = true
		}
		db, err = sql.Open("sqlite3_pq", ":memory:")
		engine = &sqliteQueryEngine{db: db}
	} else if format == "pq" {
		if !moduleRegistered {
			sql.Register("sqlite3_pq", &sqlite3.SQLiteDriver{
				ConnectHook: func(conn *sqlite3.SQLiteConn) error {
					if err := conn.RegisterFunc("text_extract", fns.TextExtract, true); err != nil {
						return err
					}
					if err := conn.RegisterFunc("regexp", fns.Regexp, true); err != nil {
						return err
					}
					if err := conn.RegisterFunc("match", fns.Matches, true); err != nil {
						return err
					}
					return conn.CreateModule("pq", &module)
				},
			})
			moduleRegistered = true
		}
		db, err = sql.Open("sqlite3_pq", ":memory:")
		if err != nil {
			return engine, err
		}
		engine = &sqlitePQQueryEngine{&module, db}
	} else if format == "file" {
		dataFile, err := ioutil.TempFile("", "pq.*.sql")
		if err != nil {
			return engine, err
		}
		db, err = sql.Open("sqlite3", dataFile.Name())
		if err != nil {
			return engine, err
		}
		engine = &sqliteQueryEngine{db: db, dbFile: dataFile}
	} else {
		err = errors.New("Unknown Engine Format - " + format)
	}
	return
}
