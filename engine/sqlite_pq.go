package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/blue4209211/pq/df"
	"github.com/mattn/go-sqlite3"
)

type sqlitePQQueryEngine struct {
	module *pqModule
	db     *sql.DB
}

func (t *sqlitePQQueryEngine) Close() {
	t.module.data = []df.DataFrame{}
	t.db.Close()
}

func (t *sqlitePQQueryEngine) Query(query string) (result df.DataFrame, err error) {
	return queryInternal(t.db, query)
}

func (t *sqlitePQQueryEngine) RegisterTable(dataFrame df.DataFrame) error {

	t.module.data = append(t.module.data, dataFrame)

	cols := dataFrame.Schema()
	if len(cols) == 0 {
		return errors.New("Columns are empty for source - " + dataFrame.Name())
	}

	err := t.createTable(dataFrame.Name(), cols)
	if err != nil {
		return err
	}

	return err
}

func (t *sqlitePQQueryEngine) createTable(tableName string, cols []df.Column) (err error) {
	sqlStmt := `create virtual table "%s" using pq (%s);`
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

type pqModule struct {
	data []df.DataFrame
}

func (t *pqModule) createTable(c *sqlite3.SQLiteConn, tableName string, cols []df.Column) (err error) {
	sqlStmt := `create table "%s" (%s);`
	columnStr := ""
	for _, col := range cols {
		columnStr = columnStr + " \"" + col.Name + "\" " + getSqliteType(col.Format) + " ,"
	}
	sqlStmt = fmt.Sprintf(sqlStmt, tableName, columnStr[0:len(columnStr)-1])
	if err != nil {
		return err
	}

	err = c.DeclareVTab(sqlStmt)

	return err
}

func (t *pqModule) Create(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {

	for _, d := range t.data {
		if d.Name() == args[2] {

			schema := d.Schema()
			err := t.createTable(c, d.Name(), schema)
			if err != nil {
				return nil, err
			}
			return &pqTable{data: &d}, nil
		}
	}

	return nil, errors.New("Table not found - " + args[2])
}

func (t *pqModule) Connect(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	return t.Create(c, args)
}

func (t *pqModule) DestroyModule() {}

type pqTable struct {
	data *df.DataFrame
}

func (t *pqTable) Open() (cur sqlite3.VTabCursor, err error) {
	return &pqCursor{0, t.data}, nil
}

func (t *pqTable) BestIndex(cstl []sqlite3.InfoConstraint, obl []sqlite3.InfoOrderBy) (*sqlite3.IndexResult, error) {
	used := make([]bool, len(cstl))
	idxStr := ""
	for c, cst := range cstl {
		if cst.Usable && cst.Op == sqlite3.OpEQ {
			used[c] = true
			idxStr = idxStr + strconv.Itoa(cst.Column) + ","
		}
	}
	if idxStr != "" {
		idxStr = idxStr[0 : len(idxStr)-1]
	}

	return &sqlite3.IndexResult{
		IdxNum: 0,
		IdxStr: idxStr,
		Used:   used,
	}, nil
}

func (t *pqTable) Disconnect() error { return nil }
func (t *pqTable) Destroy() error    { return nil }

type pqCursor struct {
	index int
	data  *df.DataFrame
}

func (t pqCursor) Column(c *sqlite3.SQLiteContext, col int) (err error) {
	cType := (*t.data).Schema()[col]
	//i, _ := cType.Format.Convert((*t.data)[t.index][col])
	i := (*t.data).Get(t.index).Data()[col]
	if i == nil {
		c.ResultNull()
		return err
	}
	switch cType.Format.Type() {
	case reflect.String:
		c.ResultText(i.(string))
	case reflect.Int64:
		c.ResultInt64(i.(int64))
	case reflect.Float64:
		c.ResultDouble(i.(float64))
	case reflect.Bool:
		c.ResultBool(i.(bool))
	}
	return nil
}

func (t *pqCursor) Filter(idxNum int, idxStr string, vals []interface{}) error {
	if idxStr == "" {
		t.index = 0
		return nil
	}

	idxStrs := strings.Split(idxStr, ",")
	idx := make([]int, len(idxStrs))

	for i, s := range idxStrs {
		idx[i], _ = strconv.Atoi(s)
	}

	var data = make([][]interface{}, 0, (*t.data).Len())
	for j := 0; j < int((*t.data).Len()); j++ {
		d := (*t.data).Get(j)
		matched := true
		for i, c := range idx {
			if d.Data()[c] != vals[i] {
				matched = false
				break
			}
		}
		if matched {
			data = append(data, d.Data())
		}
	}

	d := df.NewInmemoryDataframe((*t.data).Schema(), data)
	t.data = &d
	t.index = 0
	return nil
}

func (t *pqCursor) Next() error {
	t.index++
	return nil
}

func (t *pqCursor) EOF() bool {
	return t.index >= int((*t.data).Len())
}

func (t *pqCursor) Rowid() (int64, error) {
	return int64(t.index), nil
}

func (t *pqCursor) Close() error {
	return nil
}
