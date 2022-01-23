package engine

import (
	"strings"
)

func textExtract(col string, colIdx int) string {
	arr := strings.Split(col, " ")
	if colIdx >= len(arr) {
		return ""
	}
	return arr[colIdx]

}
