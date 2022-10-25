package df

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type intFormat struct {
	name string
}

func (t intFormat) String() string {
	return t.Name()
}

func (t intFormat) Name() string {
	return t.name
}

func (t intFormat) Type() reflect.Kind {
	return reflect.Int64
}

func (t intFormat) Convert(i any) (any, error) {
	if i == nil {
		return i, nil
	}
	return i2int(i)
}

type stringFormat struct {
	name string
}

func (t stringFormat) String() string {
	return t.Name()
}

func (t stringFormat) Name() string {
	return t.name
}

func (t stringFormat) Type() reflect.Kind {
	return reflect.String
}

func (t stringFormat) Convert(i any) (any, error) {
	if i == nil {
		return i, nil
	}
	return i2str(i)
}

type boolFormat struct {
	name string
}

func (t boolFormat) String() string {
	return t.Name()
}

func (t boolFormat) Name() string {
	return t.name
}

func (t boolFormat) Type() reflect.Kind {
	return reflect.Bool
}

func (t boolFormat) Convert(i any) (any, error) {
	if i == nil {
		return i, nil
	}
	return i2bool(i)
}

type doubleFormat struct {
	name string
}

func (t doubleFormat) String() string {
	return t.Name()
}

func (t doubleFormat) Name() string {
	return t.name
}

func (t doubleFormat) Type() reflect.Kind {
	return reflect.Float64
}

func (t doubleFormat) Convert(i any) (any, error) {
	if i == nil {
		return i, nil
	}
	return i2double(i)
}

type datetimeFormat struct {
	name string
}

func (t datetimeFormat) String() string {
	return t.Name()
}

func (t datetimeFormat) Name() string {
	return t.name
}

func (t datetimeFormat) Type() reflect.Kind {
	return reflect.Float64
}

func (t datetimeFormat) Convert(i any) (any, error) {
	if i == nil {
		return i, nil
	}
	return i2datetime(i)
}

// GetFormatFromKind returns format based on kind
func GetFormatFromKind(t reflect.Kind) (format Format, err error) {
	return GetFormat(t.String())
}

// IntegerFormat integer format
var IntegerFormat intFormat = intFormat{name: "integer"}

// StringFormat string format
var StringFormat stringFormat = stringFormat{name: "string"}

// DoubleFormat double format
var DoubleFormat doubleFormat = doubleFormat{name: "double"}

// BoolFormat bool format
var BoolFormat boolFormat = boolFormat{name: "boolean"}

// DateFormat bool format
var DateTimeFormat datetimeFormat = datetimeFormat{name: "datetime"}

// GetFormat returns format based on type
func GetFormat(t string) (format Format, err error) {
	t = strings.ToLower(t)
	if t == "string" || t == "text" {
		format = StringFormat
	} else if t == "float64" || t == "float32" || t == "double" {
		format = DoubleFormat
	} else if t == "integer" || t == "int8" || t == "int16" || t == "int32" || t == "int64" || t == "uint8" || t == "uint16" || t == "uint32" || t == "uint64" || t == "int" || t == "uint" {
		format = IntegerFormat
	} else if t == "bool" || t == "boolean" {
		format = BoolFormat
	} else if t == "date" || t == "datetime" || t == "time" {
		format = DateTimeFormat
	} else {
		err = errors.New(t)
	}
	return format, err
}

func i2datetime(v any) (datetime time.Time, err error) {
	if v == nil {
		return datetime, err
	}
	datetime, ok := v.(time.Time)
	if ok {
		return datetime, err
	}

	if reflect.TypeOf(v).String() == "time.Time" {
		datetime = v.(time.Time)
	}
	return datetime, err
}

func i2str(v any) (str string, err error) {
	if v == nil {
		return str, err
	}
	str, ok := v.(string)
	if ok {
		return str, err
	}

	vt := reflect.TypeOf(v).Kind()

	switch vt {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		rv := reflect.ValueOf(v)
		str = strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		rv := reflect.ValueOf(v)
		str = strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		rv := reflect.ValueOf(v)
		str = fmt.Sprint(rv.Float())
	case reflect.Bool:
		if v.(bool) {
			str = "true"
		} else {
			str = "false"
		}
	case reflect.String:
		str = v.(string)
	default:
		data, err := json.Marshal(v)
		if err == nil {
			str = string(data)
		}
	}

	return str, err
}

func i2int(v any) (i int64, err error) {
	if v == nil {
		return i, err
	}

	i, ok := v.(int64)
	if ok {
		return i, err
	}

	t := reflect.TypeOf(v)

	vt := t.Kind()
	switch vt {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		rv := reflect.ValueOf(v)
		i = rv.Int()
	case reflect.Float32, reflect.Float64:
		rv := reflect.ValueOf(v)
		i = int64(rv.Float())
	case reflect.Bool:
		if v.(bool) {
			i = 1
		} else {
			i = 0
		}
	case reflect.String:
		f, err := strconv.ParseFloat(v.(string), 64)
		if err == nil {
			i = int64(f)
		} else {
			return i, err
		}
	case reflect.Slice:
		byteArray := v.([]byte)
		i, err := strconv.Atoi(string(byteArray))
		if err != nil {
			return int64(i), err
		}
		return int64(i), err

	default:
		err = errors.New("unsupported type - " + vt.String())
	}

	return i, err
}

func i2double(v any) (f float64, err error) {
	if v == nil {
		return f, err
	}
	f, ok := v.(float64)
	if ok {
		return f, err
	}

	vt := reflect.TypeOf(v).Kind()
	switch vt {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		rv := reflect.ValueOf(v)
		f = float64(rv.Int())
	case reflect.Float32, reflect.Float64:
		rv := reflect.ValueOf(v)
		f = rv.Float()
	case reflect.Bool:
		if v.(bool) {
			f = 1.0
		} else {
			f = 0.0
		}
	case reflect.String:
		f, err = strconv.ParseFloat(v.(string), 64)
	default:
		err = errors.New("unsupported type - " + vt.String())
	}

	return f, err
}

func i2bool(v any) (b bool, err error) {
	if v == nil {
		return b, err
	}
	i, ok := v.(bool)
	if ok {
		return i, err
	}

	vt := reflect.TypeOf(v).Kind()
	switch vt {
	case reflect.Bool:
		b = v.(bool)
	case reflect.String:
		b, err = strconv.ParseBool(v.(string))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
	case reflect.Float32, reflect.Float64:
		rv := reflect.ValueOf(v)
		f := rv.Float()
		if f == 1 {
			b = true
		} else if f == 0 {
			b = false
		} else {
			err = errors.New("unsupported numeric value - " + fmt.Sprint(f))
		}
	case reflect.Slice:
		byteArray := v.([]byte)
		i, err := strconv.Atoi(string(byteArray))
		if err != nil {
			return b, err
		}
		if i == 1 {
			b = true
		} else if i == 0 {
			b = false
		}

	default:
		err = errors.New("unsupported type - " + vt.String())
	}
	return b, err
}

type inMemorySchema struct {
	cols []SeriesSchema
}

func (t *inMemorySchema) Series() []SeriesSchema {
	return t.cols
}
func (t *inMemorySchema) GetByName(s string) (c SeriesSchema, e error) {
	for _, c := range t.cols {
		if strings.EqualFold(c.Name, s) {
			return c, e
		}
	}
	return c, errors.New("column Not Found")
}

func (t *inMemorySchema) GetIndexByName(s string) (index int, e error) {
	for i, c := range t.cols {
		if strings.EqualFold(c.Name, s) {
			return i, e
		}
	}
	return index, errors.New("column Not Found")
}

func (t *inMemorySchema) Get(i int) SeriesSchema {
	return t.cols[i]
}

func (t *inMemorySchema) Len() int {
	return len(t.cols)
}

// NewSchema returns new schema based on given columns
func NewSchema(cols []SeriesSchema) DataFrameSchema {
	return &inMemorySchema{cols: cols}
}
