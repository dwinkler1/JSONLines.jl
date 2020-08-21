module JSONLines

import JSON3, 
    Mmap,
    Tables,
    StructTypes
import CategoricalArrays

import Base.Threads.@spawn

export readfile,
    readlazy,
    readarrays,
    reset!,
    writefile,
    @MStructType,
    select

include("helpers.jl")
include("rows.jl")
include("file.jl")
include("lazy.jl")

"""
    readfile(file::AbstractString; kwargs...) => Vector{JSON3.Object}

Read (parts of) a JSONLines file.

* `file`: Path to JSONLines file
* Keyword Arguments:
    * `structtype = nothing`: StructType passed to `JSON3.read` for each row of the file
    * `nrows = nothing`: Number of rows to load
    * `skip = nothing`: Number of rows to skip before loading
    * `usemmap::Bool = (nrows !== nothing || skip !=nothing)`: Memory map file (required for nrows and skip)
    * `nworkers::Int = 1`: Number of threads to spawn for parsing the file
"""
function readfile(file; structtype = nothing, nrows = nothing, skip = nothing, usemmap::Bool = (nrows !== nothing || skip !== nothing), nworkers = 1)
    ff = getfile(file, nrows, skip, usemmap)
    length(ff) == 0 && (return JSON3.Object[])
    if nworkers == 1
        rows = parserows(ff, structtype)
    elseif nworkers > 1
        rows = parsechunks(ff, nworkers, structtype)
    else
        throw(ArgumentError("nworkers must be >= 1"))
    end
    return rows
end

"""
    readlazy(file; returnparsed = true, structtype = nothing, skip = nothing) => JSONLines.LazyRows

Get a lazy iterator over a JSONLines file. 
`iterate(l::LazyRows)` returns a `Tuple` with the `JSON3.Object`, if `returnparsed = true`, or the UInt8 representation of the current line and the index of its last element.
A `LazyRows` object tracks its own state (which can be reset to the beginning of the file using [`reset!`](@ref)) so that it is possible to call `iterate` without additional arguments.
To materialize all elements call `[row for row in readlazy("file.jsonl")]`. 

* `file`: Path to JSONLines file
* `returnparsed = true`: If true rows are parsed to JSON3.Objects
* `structtype = nothing`: StructType passed to `JSON3.read` for each row of the file
* `skip = nothing`: Number of rows to skip at the beginning of the file
"""
function readlazy(file; returnparsed = true, structtype = nothing, skip = nothing)
    fi = Mmap.mmap(file)
    if !isnothing(skip)
        filestart = skiprows(fi, skip)
    else
        filestart = 0
    end
    returnparsed ? (rows = LazyRows{:Parsed}(fi, filestart, structtype)) : (rows = LazyRows{:Unparsed}(fi, filestart))
    return rows
end
    
"""
    writefile(file, data, mode = "w")

Write `data` to `file` in the JSONLines format.

* `file`: Path to target JSONLines file
* `data`: `Tables.jl` compatible data source
* `mode = "w"`: Mode to open the file in [see I/O and Network](https://docs.julialang.org/en/v1/base/io-network/)
"""
function writefile(file, data, mode = "w")
	if !Tables.istable(data)
		throw(ArgumentError("data needs to be compatible with the Tables interface"))
    end
	fi = open(file, mode)
	for row in Tables.rowtable(data)
		writerow(fi, row)
	end
	close(fi)
end

"""
    readarrays(file; namesline = 1, nrows = nothing, skip = nothing) 

Read a JSONLines file in which the rows are arrays. 

* `file`: JSONLines file with JSON arrays (`[val1, val2, ...]`) as rows
* Keyword Arguments:
    * `namesline = 1`: Row that contains the names of the columns
    * `nrows = nothing`: Number of rows to load
    * `skip = nothing`: Number of rows to skip before loading
"""
function readarrays(file; namesline = 1, nrows = nothing, skip = nothing)
    tups = getarrays(file, namesline, nrows, skip)
    return tups
end

end # Module 
