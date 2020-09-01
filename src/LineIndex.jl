
struct LineIndex{T}
    buf::Vector{UInt8}
    filestart::Int
    fileend::Int
    lineindex::Vector{Int}
    names::Vector{Symbol}
    lookup::Union{Dict{Int, Symbol}, Dict{Symbol, Int}}
    rowtype::Union{DataType, UnionAll}
    columntypes::OrderedDict{Symbol, Union{DataType, UnionAll, Union}}
    structtype::Union{Nothing, DataType}
    nworkers::Int
end

function LineIndex(buf::Vector{UInt8}, filestart::Int = 0, skip::Int = 0, nrows::Int = typemax(Int), structtype = nothing, schemafrom = 1:10, nworkers::Int = 1)
    fileend = lastindex(buf)
    filestart = skip > 0 ? skiprows(buf, fileend, skip, filestart) : filestart
    if filestart == fileend 
        @warn "Skipped all lines"
        return LineIndex{Missing}(buf, filestart, fileend, Int[0], Symbol[], Dict{Int, Symbol}(),  UnionAll, Dict{Symbol, Union{DataType, UnionAll}}(), nothing, nworkers)
    end
    if nworkers > 1
        # Todo issue warning about nrows being ignored
        lineindex = tindexrows(buf, fileend, nworkers)
    else
        lineindex = indexrows(buf, fileend, nrows, filestart)
    end
    row = parserow(@inbounds(@view(buf[rowindex(lineindex, 1)])), structtype)
    rowtype = typeof(row) 
    isarray = rowtype <: JSON3.Array
    if isarray
        rowtype = JSON3.Array{T, SubArray{UInt8,1,Array{UInt8,1},Tuple{UnitRange{Int}},true},Array{UInt64,1}} where T
        names = Symbol.(row)
        lineindex = lineindex[2:end]
        row = parserow(@inbounds(@view(buf[rowindex(lineindex, 1)])), structtype)
        lookup = Dict(names .=> 1:length(names))
        typelookup = Dict{Symbol, DataType}() # TODO Implement checking
    else
        typelookup = OrderedDict{Symbol, Union{DataType, UnionAll}}()
        lookup = Dict{Int, Symbol}()
        checkrows = (length(lineindex)-1) > last(schemafrom) ? schemafrom : first(schemafrom):(length(lineindex)-1)
        names = Symbol[]
        for i in checkrows # todo make input
            crow = parserow(@inbounds(@view(buf[rowindex(lineindex, i)])), structtype)
            colindex = 0
            cnames = propertynames(crow)
            cvals = [getproperty(crow, name) for name in cnames]
            for (k, v) in zip(cnames, cvals)
                colindex += 1
                if haskey(typelookup, k)
                    typelookup[k] = promote_type(typelookup[k], typeof(v))
                else
                    push!(names, k)
                    lookup[colindex] = k
                    typelookup[k] = typeof(v)
                end
            end
        end
    end
    return LineIndex{rowtype}(buf, filestart, fileend, lineindex, names, lookup, rowtype, typelookup, structtype, nworkers) 
end


"""
    LineIndex(path::String; filestart::Int = 0, skip::Int = 0, nrows::Int = typemax(Int), structtype = nothing, schemafrom::UnitRange{Int} = 1:10, nworkers::Int = 1)

Create an index of a JSONLines file at `path`
* Keyword Arguments:
    * `filestart=0`: Number of bytes to skip before reading the file
    * `skip=0`: Number of rows to skip before parsing
    * `nrows=typemax(Int)`: Maximum number of rows to index
    * `structtype=nothing`: `StructType` passed to `JSON3.read` for each row
    * `schemafrom=1:10`: Rows to parse initially to determine columnnames and columntypes
    * `nworkers=1`: Number of threads to use for operations on the `LineIndex`
"""
LineIndex(path::String; filestart::Int = 0, skip::Int = 0, nrows::Int = typemax(Int), structtype = nothing, schemafrom::UnitRange{Int} = 1:10, nworkers::Int = 1) = LineIndex(Mmap.mmap(path), filestart, skip, nrows, structtype, schemafrom, nworkers)

## Materialize
"""
    materialize(lines::LineIndex, rows::Union{UnitRange{Int}, Vector{Int}} = 1:length(lines))

Return a `Vector{NamedTuple}` of the `rows` selected. Similar to `Tables.rowtable`
"""
function materialize(lines::LineIndex, rows::Union{UnitRange{Int}, Vector{Int}} = 1:length(lines))
    if lines.nworkers > 1
        return _tmaterialize(lines.buf, lines.lineindex, rows, tuple(columnnames(lines)...), lines.structtype, lines.nworkers)
    else
        return _materialize(lines.buf, lines.lineindex, rows, tuple(columnnames(lines)...), lines.structtype)
    end
end

"""
    materialize(lines::LineIndex,  f::Function, rows::Union{UnitRange{Int}, Vector{Int}} = 1:length(lines); eltype = T where T)

Apply `f` to `rows` selected. `eltype` of result can be specified as keyword argument.
"""
function materialize(lines::LineIndex,  f::Function, rows::Union{UnitRange{Int}, Vector{Int}} = 1:length(lines); eltype = T where T)
    if lines.nworkers > 1
        return _ftmaterialize(lines.buf, f, eltype, lines.lineindex, rows, lines.structtype, lines.nworkers)
    else
        return _fmaterialize(lines.buf, f, eltype, lines.lineindex, rows, lines.structtype)
    end
end

## Columnwise
"""
    columnwise(lines::LineIndex; coltypes = lines.columntypes)

Parse `lines` to columnwise vectors. Similar to `Tables.columntable`
"""
function columnwise(lines::LineIndex; coltypes = lines.columntypes)
    _columnwise(lines, coltypes)
end

"""
    gettypes(lines::LineIndex, rows = 1:5)

Infer types of columns in `lines` based on `rows` selected. Returns `Dict` of types.
"""
function gettypes(lines::LineIndex, rows = 1:5)
    vals = lines[rows]
    lookup = Dict{Symbol, DataType}()
    for row in vals
        cnames = propertynames(row)
        cvals = [getproperty(row, name) for name in cnames]
        for (k, v) in zip(cnames, cvals)
            if haskey(lookup, k)
                lookup[k] = promote_type(lookup[k], typeof(v))
            else
                lookup[k] = typeof(v)
            end
        end
    end
    return lookup
end

"""
    settypes!(lines::LineIndex, rows::Union{UnitRange{Int}, Vector{Int}} = 1:5)

Infer types of columns in `lines` based on `rows` selected. Overwrites existing types.
"""
function settypes!(lines::LineIndex, rows::Union{UnitRange{Int}, Vector{Int}} = 1:5)
    newtypes = gettypes(lines, rows)
    for (k, v) in newtypes
        lines.columntypes[k] = v
    end
end

"""
    columntypes(lines::LineIndex)

Returns current value of columntypes of lines.
"""
columntypes(lines::LineIndex) = lines.columntypes

"""
    settype!(lines::LineIndex, p::Union{Pair{Symbol, DataType},Pair{Symbol, UnionAll}, Pair{Symbol, Union}})

Set a single columntype using `:col => Type`.
"""
function settype!(lines::LineIndex, p::Union{Pair{Symbol, DataType},Pair{Symbol, UnionAll}, Pair{Symbol, Union}}) 
    lines.columntypes[p[1]] = p[2]
end

"""
    settypes!(lines::LineIndex, d::Union{Dict{Symbol, DataType}, Dict{Symbol, UnionAll}, Dict{Symbol, Union}})

Manually set types for columns. `d` is a `Dict` in which keys are `Symbol`s corresponding to columnnames and values are the datatypes.
"""
function settypes!(lines::LineIndex, d::Union{Dict{Symbol, DataType}, Dict{Symbol, UnionAll}, Dict{Symbol, Union}})
    for (k, v) in d
        settype!(lines, k => v)
    end
end 

## Filter
"""
    filter(f::Function, lines::LineIndex)

Return rows of `lines` for which `f` evaluates to `true`
"""
function Base.filter(f::Function, lines::LineIndex)
    if lines.nworkers > 1
        return _tfilter(lines.buf, f, lines.rowtype, lines.lineindex, lines.structtype, lines.nworkers)
    else
        return _filter(lines.buf, f, lines.rowtype, lines.lineindex, lines.structtype)
    end
end

## Find
"""
    findall(f::Function, lines::LineIndex)

Return indices of `lines` for which `f` evaluates to `true`
"""
function Base.findall(f::Function, lines::LineIndex)
    # todo threaded
    return _findall(lines, f)
end

"""
    findnext(f::Function, lines::LineIndex, i::Int)

Return index of next row for which `f` returns `true` starting at row `i`
"""
function Base.findnext(f::Function, lines::LineIndex, i::Int)
    return _findnext(lines, f, i)
end

"""
    findnext(f::Function, lines::LineIndex, i::Int)

Return index of previous row for which `f` returns `true` starting at row `i`
"""
function Base.findprev(f::Function, lines::LineIndex, i::Int)
    return _findprev(lines, f, i)
end

"""
    findfirst(f::Function, lines::LineIndex)

Return index of first row for which `f` returns `true`
"""
Base.findfirst(f::Function, lines::LineIndex) = _findnext(lines, f, 1)

"""
    findlast(f::Function, lines::LineIndex)

Return index of last row for which `f` returns `true`
"""
Base.findlast(f::Function, lines::LineIndex) = _findprev(lines, f, length(lines))

## Iteration interface
function Base.iterate(lines::LineIndex, state::Int = 1; parsed::Bool = true)
    state > length(lines) && (return nothing)
    ret = @inbounds(@view(lines.buf[rowindex(lines.lineindex, state)]))
    return (parsed ? parserow(ret, lines.structtype) : ret, state + 1)
end

function Base.length(lines::LineIndex)
    return length(lines.lineindex) - 1
end 

function Base.eltype(lines::LineIndex{T}) where T
    return T
end

Base.IteratorSize(::Type{LineIndex}) = Base.HasLength()

## Indexing  interface
Base.size(lines::LineIndex) = (length(lines), length(columnnames(lines)))

@inline function Base.getindex(lines::LineIndex, i::Int; parsed::Bool = true)
    @boundscheck checkbounds(lines, i)
    ret = @inbounds(@view(lines.buf[rowindex(lines.lineindex, i)]))
    return parsed ? parserow(ret, lines.structtype) : ret
end

## Int, Symbol
Base.getindex(lines::LineIndex, i::Int, col::Symbol) = getproperty(lines[i], col)
Base.getindex(lines::LineIndex{T}, i::Int, col::Symbol) where T <: JSON3.Array = lines[i][lines.lookup[col]]

## Int, Int
Base.getindex(lines::LineIndex, I::Vararg{Int, 2}) = getproperty(lines[I[1]], lines.lookup[I[2]])
Base.getindex(lines::LineIndex{T}, I::Vararg{Int,2}) where T <: JSON3.Array = lines[I[1]][I[2]]

## UnitRange
@inline function Base.getindex(lines::LineIndex, r::UnitRange{Int})
    @boundscheck checkbounds(lines, last(r))
    return parserows(lines.buf, lines.lineindex, r, lines.rowtype, lines.structtype, lines.nworkers)
end

## UnitRange, Symbol
function Base.getindex(lines::LineIndex, r::UnitRange{Int}, col::Symbol)
    return promote_type.(materialize(lines, x->x[col], r))
end
function Base.getindex(lines::LineIndex{T}, r::UnitRange{Int}, col::Symbol) where T <: JSON3.Array
    rows = getindex(lines, r)
    return [row[lines.lookup[col]] for row in rows]
end

## UnitRang, Int
Base.getindex(lines::LineIndex, r::UnitRange{Int}, col::Int) = getindex(lines, r, lines.lookup[col])
Base.getindex(lines::LineIndex{T}, r::UnitRange{Int}, col::Int) where T <: JSON3.Array = [l[col] for l in getindex(lines, r)]

## Vector
@inline function Base.getindex(lines::LineIndex, r::Vector{Int})
    @boundscheck checkbounds(lines, maximum(r))
    return parserows(lines.buf, lines.lineindex, r, lines.rowtype, lines.structtype, lines.nworkers)
end

## Vector, Symbol
function Base.getindex(lines::LineIndex, r::Vector{Int}, col::Symbol)
    rows = getindex(lines, r)
    return @. getproperty(rows, col)
end
function Base.getindex(lines::LineIndex{T}, r::Vector{Int}, col::Symbol) where T <: JSON3.Array
    rows = getindex(lines, r)
    return [row[lines.lookup[col]] for row in rows]
end

## Vector, Int
Base.getindex(lines::LineIndex, r::Vector{Int}, i::Int) = getindex(lines, r, lines.lookup[i])
Base.getindex(lines::LineIndex{T}, r::Vector{Int}, col::Int) where T <: JSON3.Array = [l[col] for l in getindex(lines, r)]

## Colon, Symbol 
Base.getindex(lines::LineIndex, c::Colon, col::Symbol) = lines[1:end, col]

## Symbol
Base.getindex(lines::LineIndex, col::Symbol) = lines[:, col]

## 
Base.IndexStyle(::Type{LineIndex{T}}) where T = Base.IndexLinear() 
Base.firstindex(lines::LineIndex) = 1
Base.lastindex(lines::LineIndex) = length(lines)
Base.lastindex(lines::LineIndex, i::Int) = length(lines)

Base.checkbounds(lines::LineIndex, i::Int) = i > length(lines) && throw(BoundsError(lines,i))
function Base.summary(io::IO, lines::LineIndex)
    print(io, Base.dims2string(size(lines)), " ")
    Base.showarg(io, lines, true)
end

## Show
function Base.summary(lines::LineIndex) 
    io = IOBuffer()
    summary(io, lines)
    String(take!(io))
end

Base.show(io::IO, ::MIME"text/plain", lines::LineIndex{Missing}) = summary(io, lines)

function Base.show(io::IO,  ::MIME"text/plain", lines::LineIndex)
    summary(io,lines)
    print(io, ":\n")
    scrsz = displaysize(io)[1] -2
    if scrsz -2 <= 0 
        print("  \u22ee      \u22f1  ")
        return
    end
    if length(lines) <= scrsz
        x = Matrix(undef, length(lines) + 1, size(lines)[2])
        x[1, :] = columnnames(lines)
        for i in 2:length(lines)+1
            x[i, :] = [getproperty(lines[i-1], name) for name in propertynames(lines[i-1])]
        end
        Base.print_matrix(io, x, "  ", "  ")
        return nothing
    end
    halfd = div(scrsz,2)
    uphalfd = scrsz - halfd
    indices = [1:uphalfd-2..., lastindex(lines) - halfd .+ collect(2:halfd)...]
    ll = lines[indices]
    x = Matrix(undef, scrsz-1, size(lines)[2])
    x[1, :] = columnnames(lines)
    for i in 2:(uphalfd-1)
        vals = [getproperty(ll[i-1], name) for name in propertynames(ll[i-1])]
        x[i, :] = vals
    end
    for i in 2:(halfd)
        vals =  [getproperty(ll[end-halfd+i], name)  for name in propertynames(ll[end-halfd+i])]
        x[i+uphalfd-1, :] = vals
    end
    Base.print_matrix(io, x, "    ", "\t ")
end


function Base.show(io::IO,  ::MIME"text/plain", lines::LineIndex{T}) where T <: JSON3.Array
    summary(io,lines)
    print(io, ":\n")
    scrsz = displaysize(io)[1] -2
    if scrsz -2 <= 0 
        print("  \u22ee      \u22f1  ")
        return
    end
    halfd = div(scrsz,2)
    uphalfd = scrsz - halfd
    if length(lines) <= scrsz
        x = Matrix(undef, length(lines) + 1, size(lines)[2])
        x[1, :] = columnnames(lines)
        for i in 2:length(lines)+1
            x[i, :] = lines[i-1]
        end
        Base.print_matrix(io, x, "    ", "\t ")
        return nothing
    end
    indices = [1:uphalfd-2..., lastindex(lines) - halfd .+ collect(2:halfd)...]
    ll = lines[indices]
    x = Matrix(undef, scrsz-1, size(lines)[2])
    x[1, :] = columnnames(lines)
    for i in 2:(uphalfd-1)
        vals = ll[i-1]
        x[i, :] = vals
    end
    for i in 2:(halfd)
        vals =  ll[end-halfd+i]
        x[i+uphalfd-1, :] = vals
    end
    Base.print_matrix(io, x, "  ", "  ")
end

"""
    columnnames(lines::LineIndex)

Returns the columnnames of the LineIndex
"""
columnnames(lines::LineIndex) = lines.names

## Tables.jl interface

Tables.isrowtable(lines::LineIndex) = true
Tables.rowtable(lines::LineIndex) = Tables.rowtable(materialize(lines))
Tables.columntable(lines::LineIndex) = Tables.columntable(columnwise(lines))
#Tables.getcolumn(row::JSON3.Object, ::Type{T}, i::Int, nm::Symbol) where T = T(row[nm])
