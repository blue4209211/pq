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
	"github.com/blue4209211/pq/internal/log"
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

	err := t.createTable(dataFrame.Name(), schema.Series())
	if err != nil {
		return err
	}

	log.Debug("registred df - ", dataFrame.Name(), err)

	return err
}

func (t *sqlitePQQueryEngine) createTable(tableName string, cols []df.SeriesSchema) (err error) {
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

func (t *pqModule) createTable(c *sqlite3.SQLiteConn, tableName string, cols []df.SeriesSchema) (err error) {
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
			err := t.createTable(c, d.Name(), schema.Series())
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
	case 68:
		return "isnot"
	case 69:
		return "not"
	case 70:
		return "notnull"
	case 71:
		return "isnull"
	case 72:
		return "is"
	// case 150:
	// 	return "func"
	default:
		return ""
	}
}

func (t *pqTable) BestIndex(cstl []sqlite3.InfoConstraint, obl []sqlite3.InfoOrderBy) (*sqlite3.IndexResult, error) {
	used := make([]bool, len(cstl))
	idxStr := ""

	for c, cst := range cstl {
		if cst.Usable {
			used[c] = true
			opStr := opToString(cst.Op)
			if opStr == "" {
				log.Warn("No operator found for - ", cstl, obl)
				idxStr = ""
				break
			}
			idxStr = idxStr + strconv.Itoa(cst.Column) + ":" + opStr + ","
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
	//i, _ := cType.For  mat.Convert((*t.data)[t.index][col])
	i := (*t.data).GetRow(int64(t.index)).Get(col)
	if i == nil || i.IsNil() {
		c.ResultNull()
		return err
	}
	switch cType.Format {
	case df.StringFormat:
		c.ResultText(i.GetAsString())
	case df.IntegerFormat:
		c.ResultInt64(i.GetAsInt())
	case df.DoubleFormat:
		c.ResultDouble(i.GetAsDouble())
	case df.BoolFormat:
		c.ResultBool(i.GetAsBool())
	case df.DateTimeFormat:
		c.ResultText(i.GetAsDatetime().String())
	}
	return nil
}

type filterOp struct {
	idx    int
	op     string
	schema df.Format
}

func (t *pqCursor) Filter(idxNum int, filterOrderStr string, vals []any) error {

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

		d := (*t.data).WhereRow(func(dfr df.Row) bool {
			f := true
			for i, colOp := range colIdxAndOps {
				switch colOp.op {
				case "is", "=":
					f = f && (dfr.Get(colOp.idx).Get() == vals[i])
				case "isnot", "not":
					f = f && (dfr.Get(colOp.idx).Get() != vals[i])
				case "isnull":
					f = f && (dfr.Get(colOp.idx).Get() == nil)
				case "notnull":
					f = f && (dfr.Get(colOp.idx).Get() != nil)
				case "match":
					if vals[i] == nil || dfr.Get(colOp.idx) == nil {
						f = false
					}
					f = f && (fns.Matches(vals[i].(string), dfr.GetAsString(colOp.idx)))
				case "regexp":
					if vals[i] == nil || dfr.Get(colOp.idx) == nil {
						f = false
					}
					f = f && (fns.Regexp(vals[i].(string), dfr.GetAsString(colOp.idx)))
				case "like":
					if vals[i] == nil || dfr.Get(colOp.idx) == nil {
						f = false
					}
					f = f && (fns.Like(vals[i].(string), dfr.GetAsString(colOp.idx)))
				case "glob":
					if vals[i] == nil || dfr.Get(colOp.idx) == nil {
						f = false
					}
					f = f && (fns.Glob(dfr.GetAsString(colOp.idx), vals[i].(string)))
				case "<":
					if colOp.schema.Type() == reflect.Int64 {
						v, e := df.IntegerFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.GetAsInt(colOp.idx) < v.(int64))
					} else {
						v, e := df.DoubleFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.Get(colOp.idx).GetAsDouble() < v.(float64))
					}
				case "<=":
					if colOp.schema.Type() == reflect.Int64 {
						v, e := df.IntegerFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.GetAsInt(colOp.idx) <= v.(int64))
					} else {
						v, e := df.DoubleFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.GetAsDouble(colOp.idx) <= v.(float64))
					}
				case ">":
					if colOp.schema.Type() == reflect.Int64 {
						v, e := df.IntegerFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.GetAsInt(colOp.idx) > v.(int64))
					} else {
						v, e := df.DoubleFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.GetAsDouble(colOp.idx) > v.(float64))
					}
				case ">=":
					if colOp.schema.Type() == reflect.Int64 {
						v, e := df.IntegerFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.GetAsInt(colOp.idx) >= v.(int64))
					} else {
						v, e := df.DoubleFormat.Convert(vals[i])
						if e != nil {
							f = false
							break
						}
						f = f && (dfr.GetAsDouble(colOp.idx) >= v.(float64))
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
			orderOps[i] = df.SortByIndex{Series: idx, Order: order}
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
