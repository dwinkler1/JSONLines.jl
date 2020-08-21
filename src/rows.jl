## Lazy interface and methods
struct Rows
    file::Vector{UInt8}
    rowindices::Vector{Pair{Int, Int}}
end

struct Chunks
    r::Rows
    chunkindices::Vector{Pair{Int, Int}}
    Chunks(r::Rows, n::Int) = new(r, makechunks(r, n))
end

import Base: iterate, getindex, length, keys

Base.keys(r::Rows) = Base.OneTo(length(r))
Base.keys(c::Chunks) = Base.OneTo(length(c))

function Base.getindex(r::Rows, i::Int)
    if 0 >= i > length(r)
        throw(BoundsError(i, r))
    end
    @inbounds row = r.rowindices[i]
    @inbounds return r.file[row[1]:row[2]]
end

Base.getindex(r::Rows, range::UnitRange{Int}) = [r[row] for row in range]
Base.getindex(r::Rows, indvect::Vector{Int}) = [r[row] for row in indvect]
Base.getindex(r::Rows, pair::Pair{Int, Int}) = getindex(r, pair[1]:pair[2])

function Base.getindex(c::Chunks, i::Int)
    if i<0 || i > length(c)
        throw(BoundsError(i, c))
    end
    @inbounds chunk = c.chunkindices[i]
    @inbounds return c.r[chunk] 
end

function Base.iterate(r::Rows, i = 1)
    if i > length(r.rowindices) || i < 0
        return nothing
    end
    @inbounds row = r.rowindices[i]
    @inbounds return (r.file[row[1]:row[2]], i + 1)
end

function Base.iterate(c::Chunks, i = 1)
    if i > length(c) || i < 0
        return nothing
    end
    @inbounds chunk = c.chunkindices[i]
    @inbounds return(c.r[chunk], i + 1)
end

Base.length(r::Rows) = length(r.rowindices)
Base.length(c::Chunks) = length(c.chunkindices)

iseof(rowindex::Pair{Int, Int}, filelength) = rowindex[2] == filelength
isrow(rowindex::Pair{Int, Int}) = rowindex[1] < rowindex[2]

function makechunks(r::Rows, n::Int)
    len = length(r)
    if len == 1
        return [1 => 1]
    end
    if n > len
        n = len
    end
    chunksize = (len + 1) ÷ n
    chunkindices = Vector{Pair{Int, Int}}(undef, n)
    chunkindices[1] = 1 => chunksize
    for i in 2:(n-1)
        prevchunkend = chunkindices[i-1][2]
        chunkindices[i] = (prevchunkend + 1) => (prevchunkend + chunksize)
    end
    chunkindices[n] = chunkindices[n-1][2] + 1 => len 
    return chunkindices
end

function parsechunks(rows::Rows, nworkers::Int, structtype = nothing)
    len = length(rows)
    if isnothing(structtype)
        out = Vector{JSON3.Object}(undef, len)
    else
        out = Vector{structtype}(undef, len)
    end
    chunks = Chunks(rows, nworkers)
    @sync for (i, chunk) in enumerate(chunks)
        @spawn parserows!(out, chunk, structtype, chunks.chunkindices[i][1])
    end
    return out
end

function parsechunks(chunks::Chunks, structtype = nothing)
    nworkers = length(chunks)
    if isnothing(structtype)
        out = Vector{JSON3.Object}(undef, length(chunks.r))
    else
        out = Vector{structtype}(undef, length(chunks.r))
    end
    @sync for (i, chunk) in enumerate(chunks)
        @spawn parserows!(out, chunk, structtype, chunks.chunkindices[i][1])
    end
end

"""
@MStructType name fieldnames...

This macro gives a convenient syntax for declaring mutable `StructType`s for reading specific variables from a JSONLines file

* `name`: Name of the `StructType`
* `fieldnames...`: Names of the variables to be read (must be the same as in the file)

```jldoctest
julia> @MStructType mytype a

julia> x = mytype()
mytype(missing)

julia> x.a = 1
1

julia> x
mytype(1)
```
"""
macro MStructType(name, fieldnames...)
    quote 
        mutable struct $name
            $(fieldnames...)
            $(esc(name))() = new(fill(missing, $(esc(length(fieldnames))))...)
        end
        StructTypes.StructType(::Type{$(esc(name))}) = StructTypes.Mutable()
        StructTypes.names(::Type{$(esc(name))}) = tuple()
    end
end

function MStructType(name, vars...)
    eval(:(@MStructType $name $(vars...)))
end

macro select(jsonlines, vars...)
    name = gensym()
    MStructType(name, vars...)
    quote
        len = length($jsonlines)
        out = Vector{NamedTuple{$vars}}(undef, len)
        for (i, row) in enumerate($jsonlines)
            parsedrow = JSON3.read(row, $name)
            out[i] = NamedTuple{$vars}((getfield(parsedrow, field) for field in $vars))
        end
        out
    end
end

function select(jsonlines, cols...)
    eval(:(@select $jsonlines $(cols...)))
end
