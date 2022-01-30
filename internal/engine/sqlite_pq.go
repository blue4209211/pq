package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/internal/fns"
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

func (t *sqlitePQQueryEngine) RegisterDataFrame(dataFrame df.DataFrame) error {
	t.module.data = append(t.module.data, dataFrame)

	schema := dataFrame.Schema()
	if schema.Len() == 0 {
		return errors.New("Columns are empty for source - " + dataFrame.Name())
	}

	err := t.createTable(dataFrame.Name(), schema.Columns())
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
			err := t.createTable(c, d.Name(), schema.Columns())
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

func opToString(o sqlite3.Op) string {
	switch o {
	case sqlite3.OpEQ:
		return "="
	case sqlite3.OpGE:
		return ">="
	case sqlite3.OpLE:
		return "<="
	case sqlite3.OpGT:
		return ">"
	case sqlite3.OpLT:
		return "<"
	case sqlite3.OpGLOB:
		return "glob"
	case sqlite3.OpLIKE:
		return "like"
	case sqlite3.OpMATCH:
		return "match"
	case sqlite3.OpREGEXP:
		return "regexp"
	default:
		panic("not supported " + strconv.Itoa(int(o)))
	}
}

func (t *pqTable) BestIndex(cstl []sqlite3.InfoConstraint, obl []sqlite3.InfoOrderBy) (*sqlite3.IndexResult, error) {
	used := make([]bool, len(cstl))
	idxStr := ""

	for c, cst := range cstl {
		if cst.Usable {
			used[c] = true
			idxStr = idxStr + strconv.Itoa(cst.Column) + ":" + opToString(cst.Op) + ","
		}
	}

	if len(obl) > 0 {
		idxStr = idxStr + ";"
		for _, ob := range obl {
			idxStr = idxStr + strconv.Itoa(ob.Column) + ":" + strconv.FormatBool(ob.Desc) + ","
		}
	}

	if idxStr != "" {
		idxStr = idxStr[0 : len(idxStr)-1]
	}

	return &sqlite3.IndexResult{
		IdxNum:         0,
		IdxStr:         idxStr,
		Used:           used,
		AlreadyOrdered: true,
	}, nil
}

func (t *pqTable) Disconnect() error { return nil }
func (t *pqTable) Destroy() error    { return nil }

type pqCursor struct {
	index int
	data  *df.DataFrame
}

func (t pqCursor) Column(c *sqlite3.SQLiteContext, col int) (err error) {
	cType := (*t.data).Schema().Get(col)
	//i, _ := cType.Format.Convert((*t.data)[t.index][col])
	i := (*t.data).Get(int64(t.index)).Data()[col]
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

type filterOp struct {
	idx    int
	op     string
	schema df.DataFrameSeriesFormat
}

func (t *pqCursor) Filter(idxNum int, filterOrderStr string, vals []interface{}) error {

	fmt.Println("filter", filterOrderStr, vals)

	if filterOrderStr == "" {
		t.index = 0
		return nil
	}

	filterOrderStrArr := strings.Split(filterOrderStr, ";")

	filterStr := filterOrderStrArr[0]
	if filterStr != "" {
		filterArr := strings.Split(filterStr, ",")
		colIdxAndOps := make([]filterOp, len(filterArr))

		for i, idxStr := range filterArr {
			colIdxAndOp := strings.Split(idxStr, ":")
			idx, _ := strconv.Atoi(colIdxAndOp[0])
			colIdxAndOps[i] = filterOp{idx: idx, op: colIdxAndOp[1], schema: (*t.data).Schema().Get(idx).Format}
		}

		d := (*t.data).Filter(func(dfr df.DataFrameRow) bool {
			f := true
			for i, colOp := range colIdxAndOps {
				switch colOp.op {
				case "=":
					f = f && (dfr.Get(colOp.idx) == vals[i])
				case "match":
					f = f && (fns.Matches(vals[i].(string), dfr.Get(colOp.idx).(string)))
				case "regexp":
					f = f && (fns.Regexp(vals[i].(string), dfr.Get(colOp.idx).(string)))
				case "like":
					f = f && (fns.Like(vals[i].(string), dfr.Get(colOp.idx).(string)))
				case "glob":
					f = f && (fns.Glob(dfr.Get(colOp.idx).(string), vals[i].(string)))
				case "<":
					if colOp.schema.Type() == reflect.Int64 {
						v, e := df.IntegerFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.Get(colOp.idx).(int64) < v.(int64))
					} else {
						v, e := df.DoubleFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.Get(colOp.idx).(float64) < v.(float64))
					}
				case "<=":
					if colOp.schema.Type() == reflect.Int64 {
						v, e := df.IntegerFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.Get(colOp.idx).(int64) <= v.(int64))
					} else {
						v, e := df.DoubleFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.Get(colOp.idx).(float64) <= v.(float64))
					}
				case ">":
					if colOp.schema.Type() == reflect.Int64 {
						v, e := df.IntegerFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.Get(colOp.idx).(int64) > v.(int64))
					} else {
						v, e := df.DoubleFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.Get(colOp.idx).(float64) > v.(float64))
					}
				case ">=":
					if colOp.schema.Type() == reflect.Int64 {
						v, e := df.IntegerFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.Get(colOp.idx).(int64) >= v.(int64))
					} else {
						v, e := df.DoubleFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.Get(colOp.idx).(float64) >= v.(float64))
					}

				}
			}
			return f
		})

		t.data = &d
		t.index = 0
	}

	if len(filterOrderStrArr) == 2 {
		orderStr := filterOrderStrArr[1]
		orderArr := strings.Split(orderStr, ",")
		orderOps := make([]df.SortByIndex, len(orderArr))
		for i, oa := range orderArr {
			colIdxAndOp := strings.Split(oa, ":")
			idx, _ := strconv.Atoi(colIdxAndOp[0])
			order := df.SortOrderASC
			if colIdxAndOp[1] == "true" {
				order = df.SortOrderDESC
			}
			orderOps[i] = df.SortByIndex{Column: idx, Order: order}
		}

		d := (*t.data).Sort(orderOps...)

		t.data = &d
		t.index = 0
	}

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
