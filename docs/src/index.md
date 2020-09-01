```@meta
CurrentModule = JSONLines
```

# JSONLines

A simple package to read (parts of) a [JSON Lines](http://jsonlines.org/) files. The main purpose is to read files that are larger than memory. The two main functions are `LineIndex` and `LineIterator` which return an index of the rows in the given file and an iterator over the file, respectively. The `LineIndex` is `Tables.jl` compatible and can directly be piped into e.g. a DataFrame if every row in the result has the same schema (i.e. the same variables). See also `materialize` and `columnwise`. It allows memory-efficient loading of rows of a JSON Lines file. In order to select the rows `skip` and `nrows` can be used to index `nrows` rows after skipping `skip` rows. The file is `mmap`ed and only the required rows are loaded into RAM. Files must contain a valid JSON object (denoted by `{"String1":ELEMENT1, "String2":ELEMENT2, ...}`) on each line. JSON parsing is done using the [JSON3.jl](https://github.com/quinnj/JSON3.jl) package. Lines can be separated by `\n` or `\r\n` and some whitespace characters are allowed at the beginning of a line before the JSON object and the newline character (basically all that can be represented as a single `UInt8`). Typically a file would look like this: 
```
{"name":"Daniel","organization":"IMSM"}
{"name":"Peter","organization":"StatMath"}
```

There is experimental support for JSON Arrays on each line where the first line after skip contains the names of the columns.
```
["name", "organization"]
["Daniel", "IMSM"]
["Peter", "StatMath]
```
This **should** work but is not tested thoroughly.

# Getting Started

```julia-repl
(@v1.5) pkg> add JSONLines
```

# Functions

```@index
```

```@autodocs
Modules = [JSONLines]
```
