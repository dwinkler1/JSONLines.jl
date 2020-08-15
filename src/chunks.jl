
struct Chunks_priv
    lines::Vector
    indices::Vector
    nchunks::Int
    Chunks_priv(lines, indices) = new(lines, indices, length(indices))
end

struct Chunks
    rows::Vector{Vector{UInt8}}
    indices::Vector{Int}
    nchunks::Int
    structtype
end

function Chunks(rows, n::Int, structtype = nothing)
    indices = splitrows(rows, n)
    nchunks = length(indices) 
    Chunks(rows, indices, nchunks, structtype)
end

import Base: iterate
import Base: getindex
import Base: length
import Base.Threads.@spawn

Base.length(c::Chunks) = length(c.indices)

function Base.iterate(c::Chunks, i = 1)
    if i == 1
        return (parserows(c.rows[firstindex(c.rows):c.indices[1]], c.structtype), i+1)
    elseif i > c.nchunks
        return nothing
    else
        return (parserows(c.rows[(c.indices[i-1] + 1):c.indices[i]], c.structtype), i+1) 
    end
end

function Base.getindex(c::Chunks, ind::Int)
    if ind == 1
        return parserows(c.rows[firstindex(c.rows):c.indices[1]], c.structtype)
    elseif 1 > ind > length(c)
        throw(BoundsError(c, ind))
    else
        return parserows(c.rows[(c.indices[ind-1] + 1):c.indices[ind]], c.structtype)
    end
end

function Base.iterate(c::Chunks_priv, i = 1)
    if i == 1
        return (c.lines[firstindex(c.lines):c.indices[1]], i+1)
    elseif i > c.nchunks
        return nothing
    else
        return (c.lines[(c.indices[i-1]+1):c.indices[i]], i+1)
    end
end

function Base.getindex(c::Chunks_priv, i)
    return c.lines[i]
end

function splitrows(rows, n)
    len = length(rows)
    chunksize = (len + 1) ÷ n
    indices = Vector{Int}(undef, n)
    indices[1] = chunksize
    indices[n] = lastindex(rows)
    for i in 2:(n-1)
        indices[i] = indices[i-1] + chunksize
    end
    return indices
end

function parserows(rows, structtype)
    out = Vector{JSON3.Object}(undef, length(rows))
    if !isnothing(structtype)
        for (i, row) in enumerate(rows)
            out[i] = JSON3.read(row, structtype)
        end
    else
        for (i, row) in enumerate(rows)
            out[i] = JSON3.read(row)
        end
    end
    return out
end

function parsechunks(rows, nworkers, structtype)
    len = length(rows)
    out = Vector{JSON3.Object}(undef, len)
    indices = splitrows(rows, nworkers)
    chunks = Chunks_priv(rows, indices)
    @sync for (i, chunk) in enumerate(chunks)
        if i == 1
            @spawn out[1:chunks.indices[1]] = parserows(chunk, structtype)
        else
            @spawn out[(chunks.indices[i-1] + 1):chunks.indices[i]] = parserows(chunk, structtype)
        end
    end
    return out
end

function parsechunks(rows::LazyRows, nworkers, structtype)
    len = length(rows)
    out = Vector{JSON3.Object}(undef, len)
    chunks = LazyChunks(rows, nworkers)
    @sync for (i, chunk) in enumerate(chunks)
            @spawn out[chunks.chunkindices[i][1]:chunks.chunkindices[i][2]] = parserows(chunk, structtype)
        end
    return out
end

