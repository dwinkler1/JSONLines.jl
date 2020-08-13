module JSONLines

import JSON3, 
    Mmap,
    Tables

export readfile,
    writefile

include("helpers.jl")

"""
    readfile(file::AbstractString; kwargs...) => Vector{JSON3.Object}

Read (parts of) a JSONLines file.

* `file`: Path to JSONLines file
* Keyword Arguments:
    * `structtype = nothing`: StructType passed to JSON3.read for each row of the file
    * `nrows = nothing`: Number of rows to load
    * `skip = nothing`: Number of rows to skip before loading
    * `usemmap::Bool = (nrows !== nothing || skip !=nothing)`: Memory map file (required for nrows and skip)
"""
function readfile(file; structtype = nothing, nrows = nothing, skip = nothing, usemmap::Bool = (nrows !== nothing || skip !== nothing))
    ff = getfile(file, nrows, skip, usemmap)
    length(ff) == 0 && return JSON3.Object[]
    if isnothing(structtype)
        rows = JSON3.read.(lstrip.(String.(ff)))
    else
        rows = JSON3.read.(lstrip.(String.(ff)), structtype)
    end
    return rows
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
