using JSONLines
using Test, DataFrames, RDatasets

full_web = readfile("testfiles/jsonlwebsite.jsonl") |> DataFrame;
nrow_fw = nrow(full_web)

mtcars = dataset("datasets", "mtcars")
full_mtcars = readfile("testfiles/mtcars.jsonl") |> DataFrame;
# Fix R export differences
rename!(full_mtcars, :_row => :model);
rename!(full_mtcars, names(full_mtcars) .=> lowercase.(names(full_mtcars)))
rename!(mtcars, names(mtcars) .=> lowercase.(names(mtcars)))

# Read without promotion
noprom_mtcars = readfile("testfiles/mtcars.jsonl") |> DataFrame;
nrow_mt = nrow(mtcars)

oneline = readfile("testfiles/oneline.jsonl")|> DataFrame;
oneline_plus = readfile("testfiles/oneline_plus.jsonl")|> DataFrame;

escaped = readfile("testfiles/escapedeol.jsonl")|> DataFrame;

@testset "Read" begin
    @test full_web.name == ["Gilbert", "Alexa", "May", "Deloise"]
    @test full_web.wins[1] == [["straight", "7♣"], ["one pair", "10♥"]]
    @test full_web.wins[end] == [["three of a kind", "5♣"]] 
    @test full_mtcars.mpg == mtcars.mpg
    @test noprom_mtcars.cyl[32] == 4
    @test noprom_mtcars.wt[30] == 2.77
    @test noprom_mtcars.qsec[16] == 17.82
    @test noprom_mtcars[!, :_row][16] == "Lincoln Continental"
    @test noprom_mtcars[!, :drat] == mtcars[!, :drat]
    @test oneline.name == ["Daniel"]
    @test oneline_plus.name == ["Daniel"]
    @test nrow(escaped) == 4
    @test escaped.name[1] == "Daniel\n"
    @test escaped.age[2] == "}"
end

@testset "Read lazy" begin
    @test [x for x in readlazy("testfiles/jsonlwebsite.jsonl")] |> DataFrame == full_web
    @test [x for x in readlazy("testfiles/mtcars.jsonl")] |> DataFrame == noprom_mtcars
    @test [x for x in readlazy("testfiles/oneline.jsonl")] |> DataFrame== oneline
    @test [x for x in readlazy("testfiles/oneline_plus.jsonl")]  |> DataFrame == oneline_plus
    @test [x for x in readlazy("testfiles/escapedeol.jsonl")]  |> DataFrame == escaped
end

@testset "Mmap Full File" begin
# full file equal
    @test readfile("testfiles/jsonlwebsite.jsonl", usemmap = true) |> DataFrame == full_web
    @test readfile("testfiles/mtcars.jsonl", usemmap = true) |> DataFrame == noprom_mtcars
    @test readfile("testfiles/oneline.jsonl", usemmap = true) |> DataFrame== oneline
    @test readfile("testfiles/oneline_plus.jsonl", usemmap = true)  |> DataFrame == oneline_plus
    @test readfile("testfiles/escapedeol.jsonl", usemmap = true)  |> DataFrame == escaped
end

@testset "Read nworkers" begin
# full file equal
    @test readfile("testfiles/jsonlwebsite.jsonl", nworkers = 2) |> DataFrame == full_web
    @test readfile("testfiles/mtcars.jsonl", nworkers = 3) |> DataFrame == noprom_mtcars
    @test readfile("testfiles/oneline.jsonl", nworkers = 5) |> DataFrame== oneline
    @test readfile("testfiles/oneline_plus.jsonl", nworkers = Base.Threads.nthreads())  |> DataFrame == oneline_plus
    @test readfile("testfiles/escapedeol.jsonl", nworkers = Base.Threads.nthreads() + 1)  |> DataFrame == escaped
end

@testset "Read nworkers mmap" begin
# full file equal
    @test readfile("testfiles/jsonlwebsite.jsonl", nworkers = 2, usemmap = true) |> DataFrame == full_web
    @test readfile("testfiles/mtcars.jsonl", nworkers = 3, usemmap = true) |> DataFrame == noprom_mtcars
    @test readfile("testfiles/oneline.jsonl", nworkers = 5, usemmap = true) |> DataFrame== oneline
    @test readfile("testfiles/oneline_plus.jsonl", nworkers = Base.Threads.nthreads(), usemmap = true)  |> DataFrame == oneline_plus
    @test readfile("testfiles/escapedeol.jsonl", nworkers = Base.Threads.nthreads() + 1, usemmap = true)  |> DataFrame == escaped
end

@testset "skip & nrows" begin
# skip + nrows = nrow(file)
    @test readfile("testfiles/jsonlwebsite.jsonl", skip = 1, nrows = nrow_fw-1)  |> DataFrame == full_web[2:end, :]
    @test readfile("testfiles/mtcars.jsonl", skip = 2, nrows = nrow_mt-2)  |> DataFrame == noprom_mtcars[3:end, :]

# skip + nrows < nrow(file)
    @test readfile("testfiles/jsonlwebsite.jsonl", skip = 1, nrows = 2)  |> DataFrame == full_web[2:3, :]
    @test readfile("testfiles/mtcars.jsonl", skip = 2, nrows = 2)  |> DataFrame == noprom_mtcars[3:4, :]
    @test readfile("testfiles/escapedeol.jsonl", skip = 2, nrows = 1)  |> DataFrame == escaped[3:3, :]

# skip + nrows > nrow(file) (through nrow)
    @test readfile("testfiles/jsonlwebsite.jsonl", skip = 1, nrows = nrow_fw)  |> DataFrame == full_web[2:end, :]
    @test readfile("testfiles/mtcars.jsonl", skip = 12, nrows = nrow_mt + 10)  |> DataFrame == noprom_mtcars[13:end, :]
    @test readfile("testfiles/oneline.jsonl", skip = 0, nrows = 5)  |> DataFrame == oneline
    @test readfile("testfiles/oneline_plus.jsonl", skip = 0, nrows = 2)  |> DataFrame == oneline_plus
    @test readfile("testfiles/escapedeol.jsonl", skip = 2, nrows = 10)  |> DataFrame == escaped[3:end, :]

# skip + nrows > nrow(file) (through skip)
    @test readfile("testfiles/jsonlwebsite.jsonl", skip = nrow_fw+1, nrows = 1)  |> DataFrame == DataFrame()
    @test readfile("testfiles/mtcars.jsonl", skip = nrow_mt +12, nrows = 120)  |> DataFrame ==  DataFrame()
    @test readfile("testfiles/oneline.jsonl", skip = 2, nrows = 10)  |> DataFrame == DataFrame()
    @test readfile("testfiles/oneline_plus.jsonl", skip = 2, nrows = 123)  |> DataFrame == DataFrame()
    @test readfile("testfiles/escapedeol.jsonl", skip = 5, nrows = 1)  |> DataFrame == DataFrame()

# skip = nrow(file) + nrows > 0
    @test readfile("testfiles/jsonlwebsite.jsonl", skip = nrow_fw, nrows = 10)  |> DataFrame == DataFrame()
    @test readfile("testfiles/mtcars.jsonl", skip = nrow_mt, nrows = 1)  |> DataFrame == DataFrame()
    @test readfile("testfiles/oneline.jsonl", skip = 1, nrows = 12) |> DataFrame == DataFrame()
    @test readfile("testfiles/oneline_plus.jsonl", skip = 1, nrows = 1) |> DataFrame == DataFrame()
    @test readfile("testfiles/escapedeol.jsonl", skip = 4, nrows = 1) |> DataFrame == DataFrame()
end

@testset "skip" begin
# skip = nrow(file)
    @test readfile("testfiles/jsonlwebsite.jsonl", skip = nrow_fw)  |> DataFrame == DataFrame()
    @test readfile("testfiles/mtcars.jsonl", skip = nrow_mt)  |> DataFrame == DataFrame()
    @test readfile("testfiles/oneline.jsonl", skip = 1)  |> DataFrame == DataFrame()
    @test readfile("testfiles/oneline_plus.jsonl", skip = 1)  |> DataFrame == DataFrame()
    @test readfile("testfiles/escapedeol.jsonl", skip = 4)  |> DataFrame == DataFrame()

# skip > nrow(file)
    @test readfile("testfiles/jsonlwebsite.jsonl", skip = nrow_fw + 1)  |> DataFrame == DataFrame()
    @test readfile("testfiles/mtcars.jsonl", skip = nrow_mt + 42) |> DataFrame  == DataFrame()
    @test readfile("testfiles/mtcars.jsonl", skip = typemax(Int))  |> DataFrame == DataFrame()
    @test readfile("testfiles/oneline.jsonl", skip = 2)  |> DataFrame == DataFrame()
    @test readfile("testfiles/oneline_plus.jsonl", skip = 2)  |> DataFrame == DataFrame()
    @test readfile("testfiles/escapedeol.jsonl", skip = 5) |> DataFrame  == DataFrame()
    
# skip < nrow(file)
    @test readfile("testfiles/jsonlwebsite.jsonl", skip = nrow_fw - 1)  |> DataFrame == full_web[end:end, :]
    @test readfile("testfiles/mtcars.jsonl", skip = nrow_mt - 12)  |> DataFrame == noprom_mtcars[(end-11):end, :]
    @test readfile("testfiles/escapedeol.jsonl", skip = 2)  |> DataFrame == escaped[3:end, :]
end

@testset "nrows" begin
# nrows < nrow(file)
    @test readfile("testfiles/jsonlwebsite.jsonl", nrows = 2)  |> DataFrame == full_web[begin:2, :]
    @test readfile("testfiles/mtcars.jsonl", nrows = 12)  |> DataFrame == noprom_mtcars[begin:12, :]
    @test readfile("testfiles/escapedeol.jsonl", nrows = 3)  |> DataFrame == escaped[begin:3, :]

# nrows = nrow(file)
    @test readfile("testfiles/jsonlwebsite.jsonl", nrows = nrow_fw)  |> DataFrame == full_web
    @test readfile("testfiles/mtcars.jsonl", nrows = nrow_mt)  |> DataFrame == noprom_mtcars
    @test readfile("testfiles/oneline.jsonl", nrows = 1)  |> DataFrame == oneline
    @test readfile("testfiles/oneline_plus.jsonl", nrows = 1)  |> DataFrame == oneline_plus
    @test readfile("testfiles/escapedeol.jsonl", nrows = 4)  |> DataFrame == escaped

# nrows > nrow(file)
    @test readfile("testfiles/jsonlwebsite.jsonl", nrows = nrow_fw+1)  |> DataFrame == full_web
    @test readfile("testfiles/mtcars.jsonl", nrows = nrow_mt+100)  |> DataFrame == noprom_mtcars
    @test readfile("testfiles/oneline.jsonl", nrows = 2)  |> DataFrame == oneline
    @test readfile("testfiles/oneline_plus.jsonl", nrows = 2)  |> DataFrame == oneline_plus
    @test readfile("testfiles/escapedeol.jsonl", nrows = 5)  |> DataFrame == escaped
end

writefile("full_web.jsonl", full_web)
writefile("full_mtcars.jsonl", full_mtcars)
writefile("oneline2.jsonl", oneline)
writefile("oneline_plus2.jsonl", oneline_plus)
writefile("escaped2.jsonl", escaped)
@testset "write" begin
    @test readfile("full_web.jsonl") |> DataFrame == full_web
    @test readfile("full_mtcars.jsonl") |> DataFrame == full_mtcars
    @test readfile("oneline2.jsonl") |> DataFrame == oneline
    @test readfile("oneline_plus2.jsonl") |> DataFrame == oneline_plus
    @test readfile("escaped2.jsonl") |> DataFrame == escaped
end

# Cleanup
rm("full_web.jsonl")
rm("full_mtcars.jsonl")
rm("oneline2.jsonl")
rm("oneline_plus2.jsonl")
rm("escaped2.jsonl")
