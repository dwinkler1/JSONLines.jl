const _LSEP = UInt8('\n')
const _EOL = UInt8('}')
const _BOL = UInt8('{')
const _INT_MAX = typemax(Int)

# Detect space in UInt8
import Base: isspace
@inline isspace(i::UInt8) = 
    i == 0x20 || 0x09 <= i <= 0x0d || i == 0x85 || i == 0xa0

## Reading
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
    fi = read(file);
    splits = Int[]
    start = firstindex(fi)
    len = lastindex(fi)
    push!(splits, start)
    cur = detecteol(fi, start)
    isnothing(cur) ? push!(splits, len) : push!(splits, cur)
    while cur != len && !isnothing(cur) 
        cur = detecteol(fi, cur)
        isnothing(cur) ? push!(splits, len) : push!(splits, cur)
    end
    return splitfi(fi, splits)
end

# Line detection 
function checkeol(fi, cur)
    while isspace(@inbounds fi[prevind(fi, cur)])
        cur = prevind(fi, cur)
    end
    return @inbounds fi[prevind(fi, cur)] == _EOL
end

function detecteol(fi, cur)
    cur = findnext(isequal(_LSEP), fi, nextind(fi, cur))
    if cur === nothing
        return cur
    end
    iseol = checkeol(fi, cur)
    if !iseol
        detecteol(fi, cur)
    else
        return cur
    end
end

# Line splitter
function splitfi(fi, indices)
    out = Vector{Vector{UInt8}}(undef, length(indices)-1)
    @inbounds tmp = fi[indices[1]:indices[2]]
    idx = findfirst(isequal(_BOL), tmp)
    @inbounds out[firstindex(out)] = tmp[idx:end]
    for line in 3:lastindex(indices)
        @inbounds tmp = fi[nextind(fi, indices[prevind(indices, line)]):indices[line]]
        idx = findfirst(isequal(_BOL), tmp)
        @inbounds out[line-1] = tmp[idx:end]
    end
    return out
end

# Mmap main
function mmapstr(file, nlines::Int, skip::Int)
    @assert nlines > 0 "nlines must be positive"
    splits = Int[]
    fi = Mmap.mmap(file);
    len = lastindex(fi)
    skip > len && (return Vector{UInt8}[])
    cur = detecteol(fi, firstindex(fi))
    if skip == 0
        start = firstindex(fi)
        if cur == len || isnothing(cur) 
            return [fi]
        end
    elseif skip > 0
        if cur == len || isnothing(cur)
            return Vector{UInt8}[]
        end
        for _ in 2:skip 
            cur = detecteol(fi, cur)
            if cur == len || isnothing(cur)
                return Vector{UInt8}[]
            end
        end
        start = nextind(fi, cur)
        cur = detecteol(fi, cur)
        if cur == len || isnothing(cur)
            return [@inbounds fi[findnext(isequal(_BOL), fi, start):lastindex(fi)]]
        end
    else
        start = firstindex(fi)
        @warn "Ignoring skip value: $skip"
    end

    nlines == 1 && (return [@inbounds fi[findnext(isequal(_BOL), fi, start):prevind(fi, cur)]])

    append!(splits, [start, cur])
    if cur < len 
        for index in 2:nlines
            cur = detecteol(fi, cur)
            isnothing(cur) ? push!(splits, len) : push!(splits, cur)
            if cur == len || isnothing(cur)
                return splitfi(fi, splits)
            end #if 
        end #for 
    end #if 
    return splitfi(fi, splits)
end

## Writing
# Write single row
function writerow(io::IO,row)
	JSON3.write(io, row)
	write(io, '\n')
end