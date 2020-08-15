const _LSEP = UInt8('\n')
const _EOL = UInt8('}')
const _BOL = UInt8('{')
const _INT_MAX = typemax(Int)

struct LazyRows
    file::Vector{UInt8}
    rowindices::Vector{Pair{Int, Int}}
end

struct LazyChunks
    r::LazyRows
    chunkindices::Vector{Pair{Int, Int}}
    LazyChunks(r::LazyRows, n::Int) = new(r, makechunks(r, n))
end

import Base: iterate, getindex, length, keys

Base.keys(r::LazyRows) = Base.OneTo(length(r))
Base.keys(c::LazyChunks) = Base.OneTo(length(c))

function Base.getindex(r::LazyRows, i::Int)
    if 0 >= i > length(r)
        throw(BoundsError(i, r))
    end
    @inbounds row = r.rowindices[i]
    @inbounds return r.file[row[1]:row[2]]
end

Base.getindex(r::LazyRows, range::UnitRange{Int}) = [r[row] for row in range]
Base.getindex(r::LazyRows, indvect::Vector{Int}) = [r[row] for row in indvect]
Base.getindex(r::LazyRows, pair::Pair{Int, Int}) = getindex(r, pair[1]:pair[2])

function Base.getindex(c::LazyChunks, i::Int)
    if i<0 || i > length(c)
        throw(BoundsError(i, c))
    end
    @inbounds chunk = c.chunkindices[i]
    @inbounds return c.r[chunk] 
end

function Base.iterate(r::LazyRows, i = 1)
    if i > length(r.rowindices) || i < 0
        return nothing
    end
    @inbounds row = r.rowindices[i]
    @inbounds return (r.file[row[1]:row[2]], i + 1)
end

function Base.iterate(c::LazyChunks, i = 1)
    if i > length(c) || i < 0
        return nothing
    end
    @inbounds chunk = c.chunkindices[i]
    @inbounds return(c.r[chunk], i + 1)
end

Base.length(r::LazyRows) = length(r.rowindices)
Base.length(c::LazyChunks) = length(c.chunkindices)

function detectrow(file::Vector{UInt8}, prevend::Int)
    searchstart = nextind(file, prevend)
    rowstart = findnext(isequal(_BOL), file, searchstart)
    rowend = findnext(isequal(_LSEP), file, searchstart)
    if isnothing(rowstart)
        rowstart = lastindex(file)
    end
    if isnothing(rowend)
        rowend = lastindex(file)
    end
    return rowstart => rowend
end

function skiprows(file::Vector{UInt8}, n::Int, prevend::Int = 0)
    ind = nextind(file, prevend)
    for _ in 1:n
        if isnothing(ind)
            return lastindex(file)
        end
        ind = findnext(isequal(_LSEP), file, nextind(file, ind))
    end
    if isnothing(ind)
        return lastindex(file)
    end
    return ind
end

iseof(rowindex::Pair{Int, Int}, filelength) = rowindex[2] == filelength
isrow(rowindex::Pair{Int, Int}) = rowindex[1] < rowindex[2]

function mmaplazy(file, nlines, skip)
    fi = Mmap.mmap(file);
    len = lastindex(fi)
    rowindices = Pair{Int, Int}[]
    if skip > 0
        filestart = skiprows(fi, skip, 0)
        if filestart == len
            return LazyRows(UInt8[], Pair{Int, Int}[])
        end
    else 
        filestart = 0
    end
    row = detectrow(fi, filestart)
    if isrow(row)
        push!(rowindices, row)
    end
    if iseof(row, len)
        return LazyRows(fi, rowindices)
    end
    for rowi in 2:nlines
        row = detectrow(fi, rowindices[rowi-1][2])
        if isrow(row)
            push!(rowindices, row)
        end
        if iseof(row, len)
            return LazyRows(fi, rowindices)
        end
    end
    return LazyRows(fi, rowindices)
end

function makechunks(r::LazyRows, n::Int)
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