package rdbms

import (
	"context"
	"fmt"
	"os"
	"testing"

	"database/sql"

	// initialize sqlite3
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestRdbmsReader(t *testing.T) {
	tempfile, err := os.CreateTemp("", "test*.sql")
	assert.NoError(t, err)
	defer tempfile.Close()

	db, err := sql.Open("sqlite3", tempfile.Name())
	assert.NoError(t, err)

	db.Exec("create table t_123 (a text, b integer, c double, d boolean)")
	r, err := db.Exec("insert into t_123 values('1', 123, 123.123, false)")
	assert.NoError(t, err)

	c, err := r.RowsAffected()
	assert.Equal(t, int64(1), c)
	assert.NoError(t, err)

	source := DataSource{}

	isSupported := source.IsSupported("sqlite")
	assert.Equal(t, isSupported, true)

	data, err := source.Read(context.TODO(), fmt.Sprintf("sqlite:%s?db.query=select * from t_123#t", tempfile.Name()), map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "t", data.Name())
	assert.Equal(t, 4, data.Schema().Len())
	assert.Equal(t, "a", data.Schema().Get(0).Name)
	assert.Equal(t, "string", data.Schema().Get(0).Format.Name())
	assert.Equal(t, int64(1), data.Len())

}
