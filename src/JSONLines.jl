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
    settypes!,
    settype!,
    columntypes,
    filter,
    findall,
    findnext,
    findfirst,
    findprev,
    findlast

include("LineIterator.jl")
include("LineIndex.jl")
include("utils.jl")
include("sugar.jl")
include("write.jl")
end