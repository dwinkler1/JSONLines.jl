var documenterSearchIndex = {"docs":
[{"location":"","page":"Home","title":"Home","text":"CurrentModule = JSONLines","category":"page"},{"location":"#JSONLines","page":"Home","title":"JSONLines","text":"","category":"section"},{"location":"","page":"Home","title":"Home","text":"A simple package to read (parts of) a JSON Lines files to a vector of JSON3.Objects. This vector is Tables.jl compatible and can directly be piped into e.g. a DataFrame if every row in the result has the same schema (i.e. the same variables). It allows memory-efficient loading of rows of a JSON Lines file. In order to select the rows skip and nrows can be used to load nrows rows after skipping skip rows. The file is mmaped and only the required rows are loaded into RAM. Files must contain a valid JSON object (denoted by {\"String1\":ELEMENT1, \"String2\":ELEMENT2, ...}) on each line. JSON parsing is done using the JSON3.jl package. Lines can be separated by \\n or \\r\\n and some whitespace characters are allowed between the end of the JSON object and the newline character (basically all that can be represented as a single UInt8). Typically a file would look like this: ","category":"page"},{"location":"","page":"Home","title":"Home","text":"{\"name\":\"Daniel\",\"organization\":\"IMSM\"}\n{\"name\":\"Peter\",\"organization\":\"StatMath\"}","category":"page"},{"location":"#Getting-Started","page":"Home","title":"Getting Started","text":"","category":"section"},{"location":"","page":"Home","title":"Home","text":"(@v1.5) pkg> add JSONLines","category":"page"},{"location":"#Functions","page":"Home","title":"Functions","text":"","category":"section"},{"location":"","page":"Home","title":"Home","text":"","category":"page"},{"location":"","page":"Home","title":"Home","text":"Modules = [JSONLines]","category":"page"},{"location":"#JSONLines.readfile-Tuple{Any}","page":"Home","title":"JSONLines.readfile","text":"readfile(file::AbstractString; kwargs...) => Vector{JSON3.Object}\n\nRead (parts of) a JSONLines file.\n\nfile: Path to JSONLines file\nKeyword Arguments:\nstructtype = nothing: StructType passed to JSON3.read for each row of the file\nnrows = nothing: Number of rows to load\nskip = nothing: Number of rows to skip before loading\nusemmap::Bool = (nrows !== nothing || skip !=nothing): Memory map file (required for nrows and skip)\nnworkers::Int = 1: Number of threads to spawn for parsing the file\n\n\n\n\n\n","category":"method"},{"location":"#JSONLines.readlazy-Tuple{Any}","page":"Home","title":"JSONLines.readlazy","text":"readlazy(file; returnparsed = true, structtype = nothing, skip = nothing) => JSONLines.LazyRows\n\nGet a lazy iterator over a JSONLines file.  iterate(l::LazyRows) returns a Tuple with the JSON3.Object, if returnparsed = true, or the UInt8 representation of the current line and the index of its last element. A LazyRows object tracks its own state (which can be reset to the beginning of the file using reset!) so that it is possible to call iterate without additional arguments. To materialize all elements call [row for row in readlazy(\"file.jsonl\")]. \n\nfile: Path to JSONLines file\nreturnparsed = true: If true rows are parsed to JSON3.Objects\nstructtype = nothing: StructType passed to JSON3.read for each row of the file\nskip = nothing: Number of rows to skip at the beginning of the file\n\n\n\n\n\n","category":"method"},{"location":"#JSONLines.reset!-Tuple{JSONLines.LazyRows}","page":"Home","title":"JSONLines.reset!","text":"reset!(rows::LazyRows)\n\nReset row iterator to beginning of file\n\n\n\n\n\n","category":"method"},{"location":"#JSONLines.select-Tuple{Any,Vararg{Any,N} where N}","page":"Home","title":"JSONLines.select","text":"select(jsonlines, cols...)\n\njsonlines: Iterator over unparsed JSONLines (e.g. readlazy(\"file.jsonlines\", returnparsed = false))\ncols...: Columnnames to be selected\n\n\n\n\n\n","category":"method"},{"location":"#JSONLines.writefile","page":"Home","title":"JSONLines.writefile","text":"writefile(file, data, mode = \"w\")\n\nWrite data to file in the JSONLines format.\n\nfile: Path to target JSONLines file\ndata: Tables.jl compatible data source\nmode = \"w\": Mode to open the file in see I/O and Network\n\n\n\n\n\n","category":"function"},{"location":"#JSONLines.@MStructType-Tuple{Any,Vararg{Any,N} where N}","page":"Home","title":"JSONLines.@MStructType","text":"@MStructType name fieldnames...\n\nThis macro gives a convenient syntax for declaring mutable StructTypes for reading specific variables from a JSONLines file\n\nname: Name of the StructType\nfieldnames...: Names of the variables to be read (must be the same as in the file)\n\n\n\n\n\n","category":"macro"}]
}
