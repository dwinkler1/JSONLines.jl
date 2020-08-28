
function writelines(path::String, rows; nworkers = 1, mode = "w")
    if mode ∉ ["a", "a+", "w", "w+", "r+"]
        throw(ArgumentError("Cannot open file in mode: $mode"))
    end
    if mode == "r+" && !isfile(path)
        throw(ArgumentError("File $path does not exist"))
    end
    if !ispath(dirname(abspath(path)))
        throw(ArgumentError("""opening file "$path": No such file or directory"""))
    end
    if nworkers > 1
        rows = Tables.rowtable(rows)
        _twriterows(path, rows, nworkers, mode)
    else
        rows = Tables.namedtupleiterator(rows)
        _writerows(path, rows, mode)
    end
end

function _twriterows(path::String, rows, nworkers, mode)
    len = length(rows)
    parts = partitionrows(len, nworkers)
    bufs = [IOBuffer() for _ in 1:nworkers]
    @sync for i in 1:nworkers
        @spawn begin
            crows = @inbounds(rows[parts[i]])
            for row in crows
                JSON3.write(bufs[i], row)
                write(bufs[i], '\n')
            end
        end
    end
    open(path, mode) do io
        for buf in bufs
            write(io, take!(buf))
        end
    end
end

function _writerows(path, rows, mode)
    open(path, mode) do io
        for row in rows
            JSON3.write(io, row)
            write(io, '\n')
        end
    end
end