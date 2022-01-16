package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/blue4209211/pq/df"
	"github.com/blue4209211/pq/engine"
	"github.com/blue4209211/pq/log"
	"github.com/blue4209211/pq/sources"
)

func main() {

	startTime := time.Now()

	defer func() {
		elaspedTime := time.Since(startTime)
		log.Info("Execution Time %s", elaspedTime)
	}()

	confInputCSVSep := flag.String("input."+sources.ConfigCsvSep, ",", "CSV File Seprator")
	confInputCSVHeader := flag.Bool("input."+sources.ConfigCsvHeader, true, "First Line as Header")
	confInputJSONSingleLine := flag.Bool("input."+sources.ConfigJSONSingleLine, true, "Parse JSON in multiline mode")
	confInputStdType := flag.String("input."+sources.ConfigStdType, "json", "Format for Reading from Std(console)")

	confOutputStdType := flag.String("output."+sources.ConfigStdType, "json", "Format for Writing to Std(console)")
	confOutputCSVSep := flag.String("output."+sources.ConfigCsvSep, ",", "CSV File Seprator")
	confOutputCSVHeader := flag.Bool("output."+sources.ConfigCsvHeader, true, "First Line as Header")
	confOutputJSONSingleLine := flag.Bool("output."+sources.ConfigJSONSingleLine, true, "Parse JSON in multiline mode")

	confOutputfile := flag.String("output", "-", "Resoult Output, Defaults to Stdout")
	confLoggerName := flag.String("logger", "info", "Logger - debug/info/warning/error")

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
	inputConfig[sources.ConfigCsvSep] = *confInputCSVSep
	inputConfig[sources.ConfigCsvHeader] = strconv.FormatBool(*confInputCSVHeader)
	inputConfig[sources.ConfigJSONSingleLine] = strconv.FormatBool(*confInputJSONSingleLine)
	inputConfig[sources.ConfigStdType] = *confInputStdType

	outputConfig := map[string]string{}
	outputConfig[sources.ConfigCsvSep] = *confOutputCSVSep
	outputConfig[sources.ConfigCsvHeader] = strconv.FormatBool(*confOutputCSVHeader)
	outputConfig[sources.ConfigJSONSingleLine] = strconv.FormatBool(*confOutputJSONSingleLine)
	outputConfig[sources.ConfigStdType] = *confOutputStdType

	log.Debug("input configs - %s", inputConfig)
	for i, f := range fileNames {
		if f != "-" {
			f, err := filepath.Abs(f)
			if err != nil {
				log.Error("Unable to Read Path - %s, %s", f, err)
				os.Exit(1)
			}
		}
		fileNames[i] = f
	}

	log.Debug("files - %s", fileNames)

	df, err := engine.QueryFiles(query, fileNames, inputConfig)
	if err != nil {
		log.Error("Error - %s", err)
		os.Exit(1)
	}

	writeRespose(df, outputConfig, *confOutputStdType, *confOutputfile)

}

func writeRespose(data df.DataFrame, config map[string]string, format string, outputFile string) (err error) {

	var writerBuf io.Writer
	if outputFile != "" && outputFile != "-" {
		file, err := os.Create(outputFile)
		if err != nil {
			return err
		}
		defer file.Close()
		writerBuf = file
	} else {
		log.Info("Response =")
		writerBuf = os.Stdout
	}

	source, err := sources.GetSource(format)
	if err != nil {
		log.Info("Unable to Get Source = %s", err)
	}
	writer, err := source.Writer(data, config)
	if err != nil {
		log.Info("Unable to Get Source Writer = %s", err)
	}
	err = writer.Write(writerBuf)
	if err != nil {
		log.Error("Unable to Write Response %s", err)
	}
	return
}
