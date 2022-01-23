package df

type renamedDataFrame struct {
	name   string
	source DataFrame
}

func (t *renamedDataFrame) Schema() []Column {
	return t.source.Schema()
}

func (t *renamedDataFrame) Name() string {
	return t.name
}

func (t *renamedDataFrame) Column(i int) DataFrameColumn {
	return t.source.Column(i)
}

func (t *renamedDataFrame) Get(i int) DataFrameRow {
	return t.source.Get(i)
}

func (t *renamedDataFrame) Len() int64 {
	return t.source.Len()
}

func (t *renamedDataFrame) ForEach(f ForeachDataframeData) {
	t.source.ForEach(f)
}

//NewRenameDataframe returns new dataframe with given name
func NewRenameDataframe(name string, data DataFrame) (output DataFrame) {
	return &renamedDataFrame{name: name, source: data}
}
