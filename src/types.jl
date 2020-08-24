## Three versions
### 1. Return fully parsed
### 2. Return rows indexed (return parsed/unparsed)
### 3. Retrun rows iterator (return parsed/unparsed)

struct RowIndex{T}
    buf::Vector{UInt8}
    rows::Vector{Int}
    RowType
end

struct RowIterator{T}
    buf::Vector{UInt8}
    RowType
end