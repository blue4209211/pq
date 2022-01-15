package df

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type intFormat struct {
	name string
}

func (self intFormat) String() string {
	return self.Name()
}

func (self intFormat) Name() string {
	return "integer"
}

func (self intFormat) Type() reflect.Kind {
	return reflect.Int64
}

func (self intFormat) Convert(i interface{}) (interface{}, error) {
	if i == nil {
		return i, nil
	}
	return i2int(i)
}

type stringFormat struct {
	name string
}

func (self stringFormat) String() string {
	return self.Name()
}

func (self stringFormat) Name() string {
	return "string"
}

func (self stringFormat) Type() reflect.Kind {
	return reflect.String
}

func (self stringFormat) Convert(i interface{}) (interface{}, error) {
	if i == nil {
		return i, nil
	}
	return i2str(i)
}

type boolFormat struct {
	name string
}

func (self boolFormat) String() string {
	return self.Name()
}

func (self boolFormat) Name() string {
	return "boolean"
}

func (self boolFormat) Type() reflect.Kind {
	return reflect.Bool
}

func (self boolFormat) Convert(i interface{}) (interface{}, error) {
	if i == nil {
		return i, nil
	}
	return i2bool(i)
}

type doubleFormat struct {
	name string
}

func (self doubleFormat) String() string {
	return self.Name()
}

func (self doubleFormat) Name() string {
	return "double"
}

func (self doubleFormat) Type() reflect.Kind {
	return reflect.Float64
}

func (self doubleFormat) Convert(i interface{}) (interface{}, error) {
	if i == nil {
		return i, nil
	}
	return i2double(i)
}

func GetFormatFromKind(t reflect.Kind) (format DataFrameFormat, err error) {
	return GetFormat(t.String())
}

func GetFormat(t string) (format DataFrameFormat, err error) {
	t = strings.ToLower(t)
	if t == "string" || t == "text" {
		format = stringFormat{name: "string"}
	} else if t == "float64" || t == "float32" || t == "double" {
		format = doubleFormat{name: "double"}
	} else if t == "integer" || t == "int8" || t == "int16" || t == "int32" || t == "int64" || t == "uint8" || t == "uint16" || t == "uint32" || t == "uint64" || t == "int" || t == "uint" {
		format = intFormat{name: "integer"}
	} else if t == "bool" || t == "boolean" {
		format = boolFormat{name: "boolean"}
	} else {
		err = errors.New(t)

	}
	return format, err
}

func i2str(v interface{}) (str string, err error) {
	if v == nil {
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

func i2int(v interface{}) (i int64, err error) {
	if v == nil {
		return i, err
	}
	vt := reflect.TypeOf(v).Kind()
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
	default:
		err = errors.New("unsupported type - " + vt.String())
	}

	return i, err
}

func i2double(v interface{}) (f float64, err error) {
	if v == nil {
		return f, err
	}
	vt := reflect.TypeOf(v).Kind()
	switch vt {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		rv := reflect.ValueOf(v)
		f = rv.Float()
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

func i2bool(v interface{}) (b bool, err error) {
	if v == nil {
		return b, err
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
	default:
		err = errors.New("unsupported type - " + vt.String())
	}
	return b, err
}
