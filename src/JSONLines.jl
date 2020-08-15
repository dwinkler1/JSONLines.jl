module JSONLines

import JSON3, 
    Mmap,
    Tables

import Base.Threads.@spawn

export readfile,
    readlazy,
    parserows,
    parsechunks,
    writefile

export LazyChunks

include("helpers.jl")
include("lazy.jl")
include("file.jl")

"""
    readfile(file::AbstractString; kwargs...) => Vector{JSON3.Object}

Read (parts of) a JSONLines file.

* `file`: Path to JSONLines file
* Keyword Arguments:
    * `structtype = nothing`: StructType passed to JSON3.read for each row of the file
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

function readlazy(file; nrows = nothing, skip = nothing)
    ff = getfile(file, nrows, skip, true)
    return ff
end
    
"""
    writefile(file, data, mode = "w")

Write `data` to `file` in the JSONLines format.

* `file`: Path to target JSONLines file
* `data`: `Tables.jl` compatible data source
* `mode = w`: Mode to open the file in [see I/O and Network](https://docs.julialang.org/en/v1/base/io-network/)
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

end # Module 
