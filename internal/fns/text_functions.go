package fns

import (
	"regexp"
	"strings"
)

func TextExtract(col string, colIdx int) string {
	arr := strings.Split(col, " ")
	if colIdx >= len(arr) {
		return ""
	}
	return arr[colIdx]

}

func Regexp(regex string, col string) bool {
	m, _ := regexp.MatchString(regex, col)
	return m
}

func Like(regex string, col string) bool {
	regex = strings.Replace(regex, "%", ".*", -1)
	regex = strings.Replace(regex, "_", ".+", -1)
	m, _ := regexp.MatchString(regex, col)
	return m
}

func Glob(regex string, col string) bool {
	m, _ := regexp.MatchString(regex, col)
	return m
}

func Matches(regex string, col string) bool {
	return strings.Index(col, regex) >= 0
}

//func instr

//func length

//func lower

//func ltrim

//func replace

//func rtrim

//func substr

//func substring

//func trim

//func upper

//func concat

//func abs

//func changes

//func char

//func glob

//func hex

//func like

//func likelihood

//func likely

//func soundex
