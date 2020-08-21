## File Input
# Choose loading method
function getfile(file, nlines, skip, usemmap)
    if usemmap
        isnothing(nlines) && (nlines = _INT_MAX)
        isnothing(skip) && (skip = 0)
        ff = mmaprows(file, nlines, skip)
    else
        nlines !== nothing || skip !== nothing && @warn "nlines and skip require mmap. Returning all lines."
        ff = readstr(file)
    end
    return ff
end

function getarrays(file, namesline, nlines, skip)
    if isnothing(nlines)
        nlines = _INT_MAX
    end
    if isnothing(skip)
        skip = 0
    end
    fi = Mmap.mmap(file)
    len = lastindex(fi)
    if namesline > 1
        namesbeg = skiprows(fi, namesline-1, 0)
    else
        namesbeg = 0
    end
    namesr = detectarrayrow(fi, namesbeg)
    names = tuple(Symbol.(JSON3.read(fi[namesr[1]:namesr[2]]))...)
    rowindices = Pair{Int, Int}[]
    if skip > 0
        filestart = skiprows(fi, skip, namesr[2])
        if filestart == len
            return NamedTuple{names}(tuple(fill(missing, length(names))...))
        end
    else
        filestart = namesr[2]
    end
    row = detectarrayrow(fi, filestart)
    if isrow(row)
        push!(rowindices, row)
    end
    if iseof(row, len)
        return [NamedTuple{names}(tuple(JSON3.read(fi[r[1]:r[2]])...)) for r in rowindices]
    end
    for rowi in 2:nlines
        row = detectarrayrow(fi, rowindices[rowi-1][2])
        if isrow(row)
            push!(rowindices, row)
        end
        if iseof(row, len)
            return [NamedTuple{names}(tuple(JSON3.read(fi[r[1]:r[2]])...)) for r in rowindices]
        end
    end
    return [NamedTuple{names}(tuple(JSON3.read(fi[r[1]:r[2]])...)) for r in rowindices]
end

# Read everything into ram
function readstr(file)
    fi = read(file)
    len = lastindex(fi)
    rowindices = Pair{Int, Int}[]
    row = detectrow(fi, 0)
    if isrow(row)
        push!(rowindices, row)
    end
    if iseof(row, len)
        return Rows(fi, rowindices)
    end
    while !iseof(row, len)
        row = detectrow(fi, rowindices[end][2])
        if isrow(row)
            push!(rowindices, row)
        end
    end
    return Rows(fi, rowindices)
end

# mmap file
function mmaprows(file, nlines, skip)
    fi = Mmap.mmap(file);
    len = lastindex(fi)
    rowindices = Pair{Int, Int}[]
    if skip > 0
        filestart = skiprows(fi, skip, 0)
        if filestart == len
            return Rows(UInt8[], Pair{Int, Int}[])
        end
    else 
        filestart = 0
    end
    row = detectrow(fi, filestart)
    if isrow(row)
        push!(rowindices, row)
    end
    if iseof(row, len)
        return Rows(fi, rowindices)
    end
    for rowi in 2:nlines
        row = detectrow(fi, rowindices[rowi-1][2])
        if isrow(row)
            push!(rowindices, row)
        end
        if iseof(row, len)
            return Rows(fi, rowindices)
        end
    end
    return Rows(fi, rowindices)
end
