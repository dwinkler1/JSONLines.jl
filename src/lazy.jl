
import Base: iterate, length
## Row Generator
mutable struct LazyRows
    file::Vector{UInt8}
    filestart::Int
    state::Int
    length::Union{Missing, Int}
    structtype::Union{Missing, DataType}
    LazyRows(file::Vector{UInt8}, filestart::Int = 0, structtype = missing) = new(file, filestart, filestart, missing, structtype)
end

# Line detection 
function detectrow(rows::LazyRows, prevrow::Int)
    searchstart = nextind(rows.file, prevrow)
    rowstart = findnext(isequal(_BOL), rows.file, searchstart)
    rowend = findnext(isequal(_LSEP), rows.file, searchstart)
    if isnothing(rowstart)
        rowstart = lastindex(rows.file)
    end
    if isnothing(rowend)
        rowend = lastindex(rows.file)
    end
    return rowstart:rowend
end

function Base.length(rows::LazyRows)
    if !ismissing(rows.length)
        return rows.length
    else
    count = 0
    cur = rows.filestart
    while !isnothing(cur) && cur != lastindex(rows.file)
        cur = findnext(isequal(_LSEP),rows.file, cur + 1)
        count += 1
    end
    rows.length = count
    return count
    end
end

function Base.iterate(rows::LazyRows, i = rows.state)
    nextrow = detectrow(rows, i)
    rows.state = last(nextrow)
    if first(nextrow) >= last(nextrow)
        return nothing
    end
    out = ismissing(rows.structtype) ? JSON3.read(rows.file[nextrow]) : JSON3.read(rows.file[nextrow], rows.structtype)
    return (out, rows.state)
end

"""
    reset!(rows::LazyRows)

Reset row iterator to beginning of file
"""
reset!(rows::LazyRows) = (rows.state = rows.filestart; return nothing)