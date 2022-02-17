# PQ - Commandline Sql Runner

## Syntex

```
pq [args] <sql query> <files...>
```

## Example

Run Query On StdIn
```
echo '[{"a":"1"},{"b":"2"}]' | pq 'select * from stdin' -
```

Use Nested JSON
```
echo '[{"a":"1", "b":true,"c":1, "d":[1,2,3], "e": 2.1}]' | pq  'select json_array_length(d) from stdin' -
```

Run Query From Different Files
```
pq 'select * from test1,test2' test1.json test2.csv
```

Run Query on Files and StdIn
```
pq 'select * from test1,stdin' test1.json -
```

Run Query on Files, StdIn and Stor to a file
```
pq -output=/tmp/response.json 'select * from test1,stdin' test1.json -
```

Run Query on File patterns (requires aliasing else throws error)
```
pq -output=/tmp/response.json 'select * from p1' /data/*/*/*.json#p1 -
```

Read data from nested json
```
echo '[{"a":"1", "b":true,"c":1, "d":[1,2,3], "e": "[{\"ea\":1, \"eb\":2}, {\"ea\":3, \"eb\":4}]"}]' | pq  'select json_extract(value, "$.ea") as ea from stdin, json_each(stdin.e)' -
```

Show table schema
```
echo '[{"a":"1", "b":true,"c":1, "d":[1,2,3], "e": 2.1}]' | pq  'pragma table_info("stdin")' -
```

Print Help
```
pq --help
```


## Supported Sources

### Files
- fileName (without extension) is treated as filename
- file extension is used to determine file format
- Gz compression is supported, filename should end with .gz to auto detect compression
    - for example file.json.gz, will have formate json and compression gz

### StdIn/Out
- default format is json
- to specify different format use config `-input.std.type=<supported type>` or `-output.std.type=<supported type>`

### db
- Postgres - postgresql://user:pass@localhost/mydatabase/?sslmode=disable
- Mysql - mysql://user:pass@localhost/dbname
- SQlite3 - sqlite:/path/to/file.db



## Supported Formats

### json
- Format
    - JSON source can have full json in file
    - JSON source can have newline seprated JSON array/object
    - Empty values gets converted to default for example empty value of null numeric column will become 0
    - numeric columns translates to float64
- Nested JSON is supported using json functions
- rootNode can be provided to read nested Object

### csv
- Format
    - First row is by default treated as column, If this disabled, then generated columns will follow c0, c1..
    - Single Char seprator or \t is supported
    - By default all data-type is treated as string

### xml
- Format
    - Requires element to be specified in configuration, if not defined then will use `element` as default
    - Attributes are specified by appending `_` in the start of attribute name
    - By default all data-type is treated as string

### parquet
- Format
    - Basic types supported
    - Int96, ByteArray And FixedByteArray are converted to string
    - Written data is not compressed

### log/text
- Format
    - Exposes two columns`data`, `line` which can be used for searching data, data is not stored in memory and parsed at runtime


## Supported Args

```
Usage of pq:
  -engine.storage string
        Logger - memory/file (default "pq")
  -input.csv.hasHeader
        First Line as Header (default true)
  -input.csv.sep string
        CSV File Seprator (default ",")
  -input.db.query string
        Rdbms Query
  -input.json.objectOnEachLine
        Parse JSON in multiline mode (default true)
  -input.std.type string
        Format for Reading from Std(console) (default "json")
  -input.xml.elementName string
        XML Element to use for Parsing XML file (default "element")
  -input.xml.objectOnEachLine
        Read Xml element from each line (default true)
  -logger string
        Logger - debug/info/warning/error (default "info")
  -output string
        Resoult Output, Defaults to Stdout (default "-")
  -output.csv.hasHeader
        First Line as Header (default true)
  -output.csv.sep string
        CSV File Seprator (default ",")
  -output.json.objectOnEachLine
        Parse JSON in multiline mode (default true)
  -output.std.type string
        Format for Writing to Std(console) (default "json")
  -output.xml.elementName string
        XML Element to use for Writing XML file (default "element")
  -output.xml.objectOnEachLine
        Write 1 row per each line (default true)
```


## Build or Install

```
make build
make install
make test
```

## Supported SQL Functions
- As supported by sqllite3 with json1 extension

### Addtitional Functions

#### text_extract
exposes `text_extract` function which can be used for extracting data from the column `text_extract(data, index, [seprator])`



## Improvements
- More tests on Parquet
- More tests on rdbms
- TODO testcases for operator impls in pq
- better support for json
    - Autodetetct json formatting, currently we need additional args to detect certain configs (improvements in parser)

## TODO (no specific order)
- stats function for basic exploration
- interactive mode so that we dont have to query same file multiple times
- read from external source systems
    - s3/http
- imporve query performance
- lazy read of files so that we can query on large files
