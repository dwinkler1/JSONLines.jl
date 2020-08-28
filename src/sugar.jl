
"""
@MStructType name fieldnames...

This macro gives a convenient syntax for declaring mutable `StructType`s for reading specific variables from a JSONLines file

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


macro select(path::String, nworkers, vars...)
    name = gensym(basename(path))
    MStructType(name, vars...)
    quote
       LineIndex($path, structtype = $name, nworkers = $nworkers)
    end
end

"""
    select(jsonlines, cols...)

* `jsonlines`: Iterator over unparsed JSONLines (e.g. readlazy("file.jsonlines", returnparsed = false))
* `cols...`: Columnnames to be selected
"""
function select(path::String, cols...; nworkers = 1)
    eval(:(@select $path $nworkers $(cols...)))
end
