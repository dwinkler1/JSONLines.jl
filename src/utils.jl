## TODO: Remove
const _LSEP = UInt8('\n')
const _EOL = UInt8('}')
const _BOL = UInt8('{')
const _ABOL = UInt8('[')
const _INT_MAX = typemax(Int)

const _RowType = SubArray{UInt8,1,Array{UInt8,1},Tuple{UnitRange{Int64}},true}
## Detect space in UInt8
import Base: isspace
@inline Base.isspace(c::UInt8) = 
    c == 0x20 || 0x09 <= c <= 0x0d || c == 0x85 || c == 0xa0

## Raw row utils
@inline _equaleol(c::UInt8) = (c == 0x0a) # 0x0a == UInt8('\n')
function _findnexteol(buf::Vector{UInt8}, size::Int64, index::Int64)
    while true
        if _equaleol(@inbounds(buf[index])) || isequal(index, size) 
            return index
        else
            index += 1
        end
    end
end

function skiprows(buf::Vector{UInt8}, size::Int64, n::Int64, from::Int64)
    isequal(from, size) && (return size)
    for _ in 1:n
        from = _findnexteol(buf, size, from+1)
        isequal(from, size) && (return size) 
    end
    return from
end

function partitionrows(nrows::Int64, nworkers::Int64)
    partitionsize = (nrows + 1) ÷ nworkers#cld(nrows, nworkers)
    println(partitionsize)
    parts = Vector{UnitRange{Int64}}(undef, nworkers)
    parts[1] = 1:partitionsize
    for i in 2:(nworkers-1)
        parts[i] = (((i-1) * partitionsize)+1):(i * partitionsize)
    end
    parts[nworkers] = (last(parts[nworkers-1])+1):nrows
    return parts
end

function rowindex(rows::Vector{Int64}, row::Int64)
    return (rows[row]+1):rows[row+1]
end

function indexrows(buf::T, size::Int64, nrows::Int64, start::Int64) where {T}
    rows = Vector{Int64}(undef, 1)
    rows[1] = start
    for i in 1:nrows
        start = _findnexteol(buf, size, start+1)
        push!(rows, start)
        isequal(start, size) && (return rows)
    end
    return rows
end

function partitionbuf(buf::T, size::Int64, nworkers::Int64) where {T}
    guessize = cld(size, nworkers)
    parts = Vector{UnitRange{Int64}}(undef, nworkers)
    parts[1] = 1:_findnexteol(buf, size, guessize)
    for i in 2:(nworkers - 1)
        parts[i] = (last(parts[i-1])):_findnexteol(buf, size, (i * guessize))
    end
    parts[nworkers] = (last(parts[nworkers-1])):size
    return parts[length.(parts) .> 1]
end

## Reading Utils
function _eatwhitespace(row::_RowType)
    for (i, c) in pairs(row)
        !isspace(c) && (return @inbounds(@view(row[i:end])))
    end
    @inbounds(@view(row[end:end]))
end

function parserow(row::_RowType, structtype::Nothing) 
    return JSON3.read(_eatwhitespace(row))
end

function parserow(row::_RowType, structtype::DataType) 
        return JSON3.read(_eatwhitespace(row), structtype)
end


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




function parserows!(container, rows, structtype,  startidx)
    if !isnothing(structtype)
        for (i, row) in enumerate(rows)
            idx = startidx + i - 1
            @inbounds container[idx] = JSON3.read(row, structtype)
        end
    else
        for (i, row) in enumerate(rows)
            idx = startidx + i - 1
            @inbounds container[idx] = JSON3.read(row)
        end
    end
    return nothing
end

function parserows(rows, structtype = nothing)
    if !isnothing(structtype)
        out = Vector{structtype}(undef, length(rows))
        for (i, row) in enumerate(rows)
           @inbounds out[i] = JSON3.read(row, structtype)
        end
    else
        out = Vector{JSON3.Object}(undef, length(rows))
        for (i, row) in enumerate(rows)
           @inbounds out[i] = JSON3.read(row)
        end
    end
    return out
end

JSON3.StructType(::Type{CategoricalArrays.CategoricalValue{T, M}} where {T, M}) = JSON3.StringType()

## Writing
# Write single row
function writerow(io::IO,row)
	JSON3.write(io, row)
	write(io, '\n')
end