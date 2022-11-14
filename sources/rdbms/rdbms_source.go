package rdbms

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"strings"

	// initialize PG driver
	_ "github.com/lib/pq"
	// initialize Mysql driver
	_ "github.com/go-sql-driver/mysql"
	// initialize sqlite3
	_ "github.com/mattn/go-sqlite3"

	"github.com/xo/dburl"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/df/inmemory"
	"github.com/blue4209211/pq/internal/log"
)

// ConfigDBQuery source format for StdIn/Out
const ConfigDBQuery = "db.query"

//DataSource exposes functionality to read/write RDBMS
type DataSource struct {
}

//IsSupported IsSupported returns supported protocols by rdbms sources
func (t *DataSource) IsSupported(protocol string) bool {
	return protocol == "mysql" || protocol == "maria" || protocol == "postgres" || protocol == "postgresql" || protocol == "sqlite"
}

func (t *DataSource) Read(context context.Context, dbURL string, args map[string]string) (data df.DataFrame, err error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return data, err
	}

	if u.RawQuery != "" {
		dbURL = strings.Split(dbURL, "?")[0]
	}

	if u.Fragment != "" {
		dbURL = strings.Split(dbURL, "#")[0]
	}

	//protocol specific handling !!
	dbURL = dbURL + "?sslmode=disable"
	if u.Scheme == "mysql" {
		dbURL = dbURL + "&parseTime=true"
	}

	db, err := dburl.Open(dbURL)
	if err != nil {
		return nil, err
	}
	query, ok := args[ConfigDBQuery]
	if !ok || query == "" {
		query = u.Query().Get(ConfigDBQuery)
	}
	if query == "" {
		return data, errors.New("db.query Is required")
	}

	schema, records, err := queryInternal(db, query)

	if err != nil {
		return data, err
	}

	if u.Fragment != "" {
		return inmemory.NewDataframeFromRowAndName(u.Fragment, schema, &records), nil
	}
	return inmemory.NewDataframeFromRow(schema, &records), nil

}

func (t *DataSource) Write(context context.Context, data df.DataFrame, path string, args map[string]string) (err error) {
	return errors.New("Unsupported")
}

func queryInternal(db *sql.DB, query string) (schema df.DataFrameSchema, data []df.Row, err error) {
	preparedQuery, err := db.Prepare(query)
	if err != nil {
		return schema, data, err
	}
	defer preparedQuery.Close()

	rows, err := preparedQuery.Query()
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
			if err != nil {
				return schema, data, err
			}

		}
		cols[i] = df.SeriesSchema{Name: c, Format: dfFormat}
	}

	schema = df.NewSchema(cols)
	data = make([]df.Row, 0, 100)

	for rows.Next() {
		dataRowPtrs := make([]any, len(sqlCols))
		for i := range dataRowPtrs {
			var dataCell any
			dataRowPtrs[i] = &dataCell
		}
		err = rows.Scan(dataRowPtrs...)
		if err != nil {
			return schema, data, err
		}

		dataRow := make([]df.Value, len(sqlCols))
		for i, cellPtr := range dataRowPtrs {
			v, err := cols[i].Format.Convert(*(cellPtr.(*any)))
			if err != nil {
				return schema, data, err
			}
			dataRow[i] = inmemory.NewValue(cols[i].Format, v)
		}

		data = append(data, inmemory.NewRow(&schema, &dataRow))
	}

	err = rows.Err()
	if err != nil {
		return schema, data, err
	}
	return schema, data, nil
}
