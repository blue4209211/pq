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

Read data from nexted json
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

## Supported Formats

### json
- Format
    - JSON source can have full json in file
    - JSON source can have newline seprated JSON array/object
    - Empty values gets converted to default for example empty value of null numeric column will become 0
    - numeric columns translates to float64
- Nested JSON is supported

### csv
- Format
    - First row is by default treated as column, If this disabled, then generated columns will follow c0, c1..
    - Single Char seprator or \t is supported
    - By default all data-type is treated as string


## Supported Args

```
Usage of pq:

  -input.csv.header
        First Line as Header (default true)
  -input.csv.sep string
        CSV File Seprator (default ",")
  -input.json.singleline
        Parse JSON in multiline mode
  -input.std.type string
        Format for Reading from Std(console) (default "json")
  -output.csv.header
        First Line as Header (default true)
  -output.csv.sep string
        CSV File Seprator (default ",")
  -output.json.singleline
        Parse JSON in multiline mode
  -output.std.type string
        Format for Writing to Std(console) (default "json")
  -logger string
        Logger - debug/info/warning/error (default "info")
  -output string
        Resoult Output, Defaults to Stdout (default "-")
```


## Build or Install

```
make build
make install
make test
```

## Supported SQL Functions
- As supported by sqllite3 with json1 extension

## TODO
- performance improvements
    - Benechmark existing perf
    - Improve performance
- better support for json
    - store json object instead of strings
    - Autodetetct json formatting
- more source types
    - parquet
    - xml
    - avro
- handling on unstructured data (logfiles)
    - full text search
- read from external source systems
    - s3
    - dynamodb
