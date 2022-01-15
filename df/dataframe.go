package df

import (
	"reflect"
)

type Column struct {
	Name   string
	Format DataFrameFormat
}

type DataFrameFormat interface {
	Name() string
	Type() reflect.Kind
	Convert(i interface{}) (interface{}, error)
}

type DataFrame interface {
	Data() ([][]interface{}, error)
	Schema() ([]Column, error)
	Name() string
}
