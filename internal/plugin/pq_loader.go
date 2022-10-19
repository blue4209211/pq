package plugin

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"plugin"

	"github.com/blue4209211/pq/df"
)

var supportedPlugins []string = []string{}
var loadedPlugins map[string]df.DataFrameSource = map[string]df.DataFrameSource{}

func init() {
	f, err := os.Stat("~/.pq/plugins")
	if err != nil {
		return
	}
	if !f.IsDir() {
		return
	}
	pqFs := os.DirFS("~/.pq")

	pluginFiles, err := fs.ReadDir(pqFs, "plugins")
	if err != nil {
		return
	}

	for _, s := range pluginFiles {
		if s.IsDir() {
			continue
		}
		supportedPlugins = append(supportedPlugins, s.Name())
	}
}

func loadPlugin(pluginName string) (d df.DataFrameSource, err error) {
	if val, ok := loadedPlugins[pluginName]; ok {
		return val, err
	}

	pluginPath := "~/.pq/plugins/" + pluginName
	plug, err := plugin.Open(pluginPath)
	if err != nil {
		return d, err
	}

	symDataSource, err := plug.Lookup("DataFrameSource")
	if err != nil {
		return d, err
	}

	var datasource df.DataFrameSource
	datasource, ok := symDataSource.(df.DataFrameSource)
	if !ok {
		return d, errors.New("unexpected type from module symbol")
	}

	loadedPlugins[pluginName] = datasource

	return datasource, err
}

type PQDataSourcePlugin struct {
}

func (t *PQDataSourcePlugin) IsSupported(protocol string) bool {
	for _, s := range supportedPlugins {
		if s == protocol {
			return true
		}
	}
	return false
}

func (t *PQDataSourcePlugin) Read(context context.Context, url string, args map[string]string) (data df.DataFrame, err error) {
	datasource, err := loadPlugin(url)
	if err != nil {
		return data, err
	}
	return datasource.Read(context, url, args)
}

func (t *PQDataSourcePlugin) Write(context context.Context, data df.DataFrame, path string, args map[string]string) (err error) {
	datasource, err := loadPlugin(path)
	if err != nil {
		return err
	}
	return datasource.Write(context, data, path, args)
}
