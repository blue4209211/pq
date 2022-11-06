package df

import (
	"context"
)

// DataFrameSource Provides interface for all the data sources
type DataFrameSource interface {
	Read(context context.Context, path string, args map[string]string) (DataFrame, error)
	Write(context context.Context, data DataFrame, path string, args map[string]string) error
	IsSupported(protocol string) bool
}
