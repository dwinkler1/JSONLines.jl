"""
@MStructType name fieldnames...

This macro gives a convenient syntax for declaring mutable `StructType`s for reading specific variables from a JSONLines file. Also defines `row[:col]` access for rows of the resulting type.

* `name`: Name of the `StructType`
* `fieldnames...`: Names of the variables to be read (must be the same as in the file)
"""
macro MStructType(name, fieldnames...)
quote
    mutable struct $name
        $(fieldnames...)
        $(esc(name))() = new(fill(missing, $(esc(length(fieldnames))))...)
    end
    StructTypes.StructType(::Type{$(esc(name))}) = StructTypes.Mutable()
    StructTypes.names(::Type{$(esc(name))}) = tuple()
    Base.getindex(x::$(esc(name)), col::Symbol) = getproperty(x, col)
end
end

function MStructType(name, vars...)
eval(:(@MStructType $name $(vars...)))
end


macro readcols(path::String, nworkers, cols...)
    name = gensym(basename(path))
    MStructType(name, cols...)
    quote
       LineIndex($path, structtype = $name, nworkers = $nworkers)
    end
end

"""
    readcols(path::String, cols...; nworkers = 1) => LineIndex

* `path`: Path to JSONLines file
* `cols...`: Columnnames to be selected
* Keyword Argument:
    * `nworkers=1`: Number of threads to use for operations on the resulting LineIndex
"""
function readcols(path::String, cols...; nworkers = 1)
    eval(:(@readcols $path $nworkers $(cols...)))
end
