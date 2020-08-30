module JSONLines

import JSON3, 
Mmap,
Tables,
StructTypes
import CategoricalArrays

import Base.Threads.@spawn

import Base:
    length,
    size,
    iterate, 
    IteratorSize, 
    eltype, 
    getindex, 
    firstindex,
    lastindex,
    IndexStyle,
    checkbounds,
    summary,
    filter,
    findall,
    findnext,
    findfirst,
    findprev,
    findlast

import Tables:
    istable,
    rowaccess,
    rows
    
export writelines,
    @MStructType,
    select,
    colnames,
    LineIndex,
    LineIterator,
    materialize,
    columnwise,
    gettypes,
    gettypes!,
    settype!,
    columntypes,
    filter,
    findall,
    findnext,
    findfirst,
    findprev,
    findlast

include("utils.jl")
include("LineIterator.jl")
include("LineIndex.jl")
include("sugar.jl")
include("write.jl")
end