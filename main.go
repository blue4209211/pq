package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/blue4209211/pq/internal/engine"
	"github.com/blue4209211/pq/internal/log"
	"github.com/blue4209211/pq/internal/sources/files"
	"github.com/blue4209211/pq/internal/sources/rdbms"
	"github.com/blue4209211/pq/internal/sources/std"
)

func main() {

	startTime := time.Now()
	defer func() {
		elaspedTime := time.Since(startTime)
		log.Debug("Execution Time ", elaspedTime)
	}()

	confInputCSVSep := flag.String("input."+files.ConfigCsvSep, ",", "CSV File Seprator")
	confInputCSVHeader := flag.Bool("input."+files.ConfigCsvHeader, true, "First Line as Header")
	confInputJSONSingleLine := flag.Bool("input."+files.ConfigJSONSingleLine, true, "Parse JSON in multiline mode")
	confInputStdType := flag.String("input."+std.ConfigStdType, "json", "Format for Reading from Std(console)")
	confInputXMLElementName := flag.String("input."+files.ConfigXMLElementName, "element", "XML Element to use for Parsing XML file")
	confInputXMLSingleLine := flag.Bool("input."+files.ConfigXMLSingleLine, true, "Read Xml element from each line")
	confDBQuery := flag.String("input."+rdbms.ConfigDBQuery, "", "Rdbms Query")

	confOutputStdType := flag.String("output."+std.ConfigStdType, "json", "Format for Writing to Std(console)")
	confOutputCSVSep := flag.String("output."+files.ConfigCsvSep, ",", "CSV File Seprator")
	confOutputCSVHeader := flag.Bool("output."+files.ConfigCsvHeader, true, "First Line as Header")
	confOutputJSONSingleLine := flag.Bool("output."+files.ConfigJSONSingleLine, true, "Parse JSON in multiline mode")
	confOutputXMLElementName := flag.String("output."+files.ConfigXMLElementName, "element", "XML Element to use for Writing XML file")
	confOutputXMLSingleLine := flag.Bool("output."+files.ConfigXMLSingleLine, true, "Write 1 row per each line")

	confOutputfile := flag.String("output", "-", "Resoult Output, Defaults to Stdout")
	confLoggerName := flag.String("logger", "info", "Logger - debug/info/warning/error")
	confEngineStorage := flag.String(engine.ConfigEngineStorage, "pq", "Logger - memory/file")

	flag.Parse()

	log.SetLogger(*confLoggerName)

	remainingArgs := flag.Args()

	if len(remainingArgs) < 2 {
		fmt.Println("Usage: pq [-args] <query> <files...or using - for stdin>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	query := remainingArgs[0]
	fileNames := remainingArgs[1:]
	inputConfig := map[string]string{}
	inputConfig[files.ConfigCsvSep] = *confInputCSVSep
	inputConfig[files.ConfigCsvHeader] = strconv.FormatBool(*confInputCSVHeader)
	inputConfig[files.ConfigJSONSingleLine] = strconv.FormatBool(*confInputJSONSingleLine)
	inputConfig[std.ConfigStdType] = *confInputStdType
	inputConfig[engine.ConfigEngineStorage] = *confEngineStorage
	inputConfig[files.ConfigXMLElementName] = *confInputXMLElementName
	inputConfig[files.ConfigXMLSingleLine] = strconv.FormatBool(*confInputXMLSingleLine)
	inputConfig[rdbms.ConfigDBQuery] = *confDBQuery

	outputConfig := map[string]string{}
	outputConfig[files.ConfigCsvSep] = *confOutputCSVSep
	outputConfig[files.ConfigCsvHeader] = strconv.FormatBool(*confOutputCSVHeader)
	outputConfig[files.ConfigJSONSingleLine] = strconv.FormatBool(*confOutputJSONSingleLine)
	outputConfig[std.ConfigStdType] = *confOutputStdType
	outputConfig[engine.ConfigEngineStorage] = *confEngineStorage
	outputConfig[files.ConfigXMLElementName] = *confOutputXMLElementName
	outputConfig[files.ConfigXMLSingleLine] = strconv.FormatBool(*confOutputXMLSingleLine)

	log.Debug("input configs - ", inputConfig)
	for i, f := range fileNames {
		if f != "-" {
			f, err := filepath.Abs(f)
			if err != nil {
				log.Error("Unable to Read Path -", f, err)
				os.Exit(1)
			}
		}
		fileNames[i] = f
	}

	log.Debug("files - ", fileNames)

	df, err := QuerySources(query, inputConfig, fileNames...)
	if err != nil {
		log.Error("Error - ", err)
		os.Exit(1)
	}

	err = WriteSource(df, outputConfig, *confOutputfile)

	if err != nil {
		log.Error(err)
	}
}
