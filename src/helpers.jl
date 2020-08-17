const _LSEP = UInt8('\n')
const _EOL = UInt8('}')
const _BOL = UInt8('{')
const _INT_MAX = typemax(Int)

# Detect space in UInt8
import Base: isspace
@inline Base.isspace(i::UInt8) = 
    i == 0x20 || 0x09 <= i <= 0x0d || i == 0x85 || i == 0xa0

# Line detection 
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
    out = Vector{JSON3.Object}(undef, length(rows))
    if !isnothing(structtype)
        for (i, row) in enumerate(rows)
           @inbounds out[i] = JSON3.read(row, structtype)
        end
    else
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