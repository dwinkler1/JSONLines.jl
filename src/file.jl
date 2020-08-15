## File Input
# Choose loading method
function getfile(file, nlines, skip, usemmap)
    if usemmap
        isnothing(nlines) && (nlines = _INT_MAX)
        isnothing(skip) && (skip = 0)
        ff = mmaplazy(file, nlines, skip)
    else
        nlines !== nothing || skip !== nothing && @warn "nlines and skip require mmap. Returning all lines."
        ff = readstr(file)
    end
    return ff
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
        return LazyRows(fi, rowindices)
    end
    while !iseof(row, len)
        row = detectrow(fi, rowindices[end][2])
        if isrow(row)
            push!(rowindices, row)
        end
    end
    return LazyRows(fi, rowindices)
end

# mmap file
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
