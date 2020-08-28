
struct LineIterator
    buf::Vector{UInt8}
    filestart::Int
    fileend::Int
    structtype::Union{Nothing, DataType}
    LineIterator(buf::Vector{UInt8}, 
        filestart::Int = 1,        
        structtype::Union{Nothing, DataType} = nothing,
        fileend::Int = lastindex(buf)) = 
        new(buf, filestart, fileend, structtype)
end

LineIterator(path::String; filestart = 1, structtype = nothing) = LineIterator(Mmap.mmap(path), filestart, structtype)

## Iteration interface
function Base.iterate(lines::LineIterator, state::Int = lines.filestart; parse::Bool = true)
    state == lines.fileend && (return nothing)
    eol = _findnexteol(lines.buf, lines.fileend, state+1)
    ret = @inbounds(@view(lines.buf[state:eol]))
    return (parse ? parserow(ret, lines.structtype) : ret, eol)
end

function Base.eltype(lines::LineIterator)
    lines.structtype !== nothing && (return lines.structtype)
    return typeof(lines[1])
end

Base.IteratorSize(::Type{LineIterator}) = Base.SizeUnknown()

## Indexing interface
function _getindex(lines::LineIterator, i::Int)
    @warn "Indexin LineIterators is slow. Consider using LineIndex instead." maxlog=1
    linestart = skiprows(lines.buf, lines.fileend, i-1, lines.filestart) + (i>1)
    linestart >= lines.fileend && (throw(BoundsError(lines, i)))
    lineend = _findnexteol(lines.buf, lines.fileend, linestart)
    return linestart:lineend
end

function Base.getindex(lines::LineIterator, i::Int; parsed::Bool = true)
    index = _getindex(lines, i) 
    ret = @inbounds(@view(lines.buf[index])) 
    return parsed ? parserow(ret, lines.structtype) : ret
end

Base.firstindex(lines::LineIterator) = lines.filestart

function Base.getindex(lines::LineIterator, i::Int, col::Symbol)
    return lines[i][col]
end

function Base.getindex(lines::LineIterator, I::Vararg{Int, 2})
    row = lines[I[1]]
    col = collect(keys(row))[I[2]]
    return row[col]
end

## Tables.jl interface

Tables.istable(lines::LineIterator) = true
Tables.rowaccess(lines::LineIterator) = true
Tables.rows(lines::LineIterator) = lines
Tables.materializer(lines::LineIterator) = Tables.rowtable
