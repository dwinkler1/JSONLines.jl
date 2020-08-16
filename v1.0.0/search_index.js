var documenterSearchIndex = {"docs":
[{"location":"","page":"Home","title":"Home","text":"CurrentModule = JSONLines","category":"page"},{"location":"#JSONLines","page":"Home","title":"JSONLines","text":"","category":"section"},{"location":"","page":"Home","title":"Home","text":"A simple package to read (parts of) a JSON Lines files to a vector of JSON3.Objects. This vector is Tables.jl compatible and can directly be piped into e.g. a DataFrame if every row in the result has the same schema (i.e. the same variables). It allows memory-efficient loading of rows of a JSON Lines file. In order to select the rows skip and nrows can be used to load nrows rows after skipping skip rows. The file is mmaped and only the required rows are loaded into RAM. Files must contain a valid JSON object (denoted by {\"String1\":ELEMENT1, \"String2\":ELEMENT2, ...}) on each line. JSON parsing is done using the JSON3.jl package. Lines can be separated by \\n or \\r\\n and some whitespace characters are allowed between the end of the JSON object and the newline character (basically all that can be represented as a single UInt8). Typically a file would look like this: ","category":"page"},{"location":"","page":"Home","title":"Home","text":"{\"name\":\"Daniel\",\"organization\":\"IMSM\"}\n{\"name\":\"Peter\",\"organization\":\"StatMath\"}","category":"page"},{"location":"#Getting-Started","page":"Home","title":"Getting Started","text":"","category":"section"},{"location":"","page":"Home","title":"Home","text":"(@v1.5) pkg> add JSONLines","category":"page"},{"location":"#Functions","page":"Home","title":"Functions","text":"","category":"section"},{"location":"","page":"Home","title":"Home","text":"","category":"page"},{"location":"","page":"Home","title":"Home","text":"Modules = [JSONLines]","category":"page"},{"location":"#JSONLines.readfile-Tuple{Any}","page":"Home","title":"JSONLines.readfile","text":"readfile(file::AbstractString; kwargs...) => Vector{JSON3.Object}\n\nRead (parts of) a JSONLines file.\n\nfile: Path to JSONLines file\nKeyword Arguments:\nstructtype = nothing: StructType passed to JSON3.read for each row of the file\nnrows = nothing: Number of rows to load\nskip = nothing: Number of rows to skip before loading\nusemmap::Bool = (nrows !== nothing || skip !=nothing): Memory map file (required for nrows and skip)\n\n\n\n\n\n","category":"method"},{"location":"#JSONLines.writefile","page":"Home","title":"JSONLines.writefile","text":"writefile(file, data, mode = \"w\")\n\nWrite data to file in the JSONLines format.\n\nfile: Path to target JSONLines file\ndata: Tables.jl compatible data source\nmode = w: Mode to open the file in see I/O and Network\n\n\n\n\n\n","category":"function"}]
}
