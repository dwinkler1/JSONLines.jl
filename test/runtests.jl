using JSONLines
using Test, DataFrames, JSONTables, Pipe, Tables

full_web = LineIndex("testfiles/jsonlwebsite.jsonl") |> DataFrame;
nrow_fw = nrow(full_web)

mtcars = jsontable(read("testfiles/mtcars.json")) |> DataFrame
full_mtcars = LineIndex("testfiles/mtcars.jsonl"; nworkers = 4) |> DataFrame;
@show full_mtcars
# Fix R export differences
rename!(full_mtcars, :_row => :model);
rename!(full_mtcars, names(full_mtcars) .=> lowercase.(names(full_mtcars)))
rename!(mtcars, names(mtcars) .=> lowercase.(names(mtcars)))

# Read without promotion
noprom_mtcars = LineIndex("testfiles/mtcars.jsonl") |> DataFrame;
nrow_mt = nrow(mtcars)

oneline = LineIndex("testfiles/oneline.jsonl")|> DataFrame;
oneline_plus = LineIndex("testfiles/oneline_plus.jsonl")|> DataFrame;

escaped = LineIndex("testfiles/escapedeol.jsonl")|> DataFrame;

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

@testset "LineIterator" begin
    @test [x for x in LineIterator("testfiles/jsonlwebsite.jsonl")] |> DataFrame == full_web
    @test [x for x in LineIterator("testfiles/mtcars.jsonl")] |> DataFrame == noprom_mtcars
    @test [x for x in LineIterator("testfiles/oneline.jsonl")] |> DataFrame== oneline
    @test [x for x in LineIterator("testfiles/oneline_plus.jsonl")]  |> DataFrame == oneline_plus
    @test [x for x in LineIterator("testfiles/escapedeol.jsonl")]  |> DataFrame == escaped
end

@testset "Read arrays" begin
    @test LineIndex("testfiles/array.jsonl", skip = 1)[2,:a] == 4
    @test LineIndex("testfiles/array.jsonl", skip = 1)[2,1] == 4
    @test LineIndex("testfiles/jsonlwebsitearray.jsonl")[1:end, :Score] == [24, 29, 14, 19]
end

@testset "readcols" begin
    JL = JSONLines
    webl = @pipe JL.readcols("testfiles/jsonlwebsite.jsonl", :name) |> materialize  |> DataFrame
    @test webl == full_web[:, [:name]]
    mtl = @pipe JL.readcols("testfiles/mtcars.jsonl", :gear, :hp; nworkers = 4) |> materialize |> DataFrame
    @test mtl == noprom_mtcars[:, [:gear, :hp]]
    onel = @pipe JL.readcols("testfiles/oneline.jsonl", :age) |> materialize |> DataFrame
    @test onel == oneline[:, [:age]]
    onepl = @pipe JL.readcols("testfiles/oneline_plus.jsonl", :name) |> materialize |> DataFrame
    @test onepl == oneline_plus[:, [:name]]
    escl = @pipe JL.readcols("testfiles/escapedeol.jsonl", :name) |> materialize |> DataFrame
    @test escl == escaped[:, [:name]]
end

@testset "Read nworkers" begin
# full file equal
    @test LineIndex("testfiles/jsonlwebsite.jsonl", nworkers = 2) |> DataFrame == full_web
    @test LineIndex("testfiles/mtcars.jsonl", nworkers = Base.Threads.nthreads() + 1) |> DataFrame == noprom_mtcars
    @test LineIndex("testfiles/oneline.jsonl", nworkers = 1) |> DataFrame== oneline
end

@testset "skip & nrows" begin
# skip + nrows = nrow(file)
    @test LineIndex("testfiles/jsonlwebsite.jsonl", skip = 1, nrows = nrow_fw-1)  |> DataFrame == full_web[2:end, :]
    @test LineIndex("testfiles/mtcars.jsonl", skip = 2, nrows = nrow_mt-2)  |> DataFrame == noprom_mtcars[3:end, :]

# skip + nrows < nrow(file)
    @test LineIndex("testfiles/jsonlwebsite.jsonl", skip = 1, nrows = 2)  |> DataFrame == full_web[2:3, :]
    @test LineIndex("testfiles/mtcars.jsonl", skip = 2, nrows = 2)  |> DataFrame == noprom_mtcars[3:4, :]
    @test LineIndex("testfiles/escapedeol.jsonl", skip = 2, nrows = 1)  |> DataFrame == escaped[3:3, :]

# skip + nrows > nrow(file) (through nrow)
    @test LineIndex("testfiles/jsonlwebsite.jsonl", skip = 1, nrows = nrow_fw)  |> DataFrame == full_web[2:end, :]
    @test LineIndex("testfiles/mtcars.jsonl", skip = 12, nrows = nrow_mt + 10)  |> DataFrame == noprom_mtcars[13:end, :]
    @test LineIndex("testfiles/oneline.jsonl", skip = 0, nrows = 5)  |> DataFrame == oneline
    @test LineIndex("testfiles/oneline_plus.jsonl", skip = 0, nrows = 2)  |> DataFrame == oneline_plus
    @test LineIndex("testfiles/escapedeol.jsonl", skip = 2, nrows = 10)  |> DataFrame == escaped[3:end, :]

# skip + nrows > nrow(file) (through skip)
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/jsonlwebsite.jsonl", skip = nrow_fw+1, nrows = 1))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/mtcars.jsonl", skip = nrow_mt +12, nrows = 120))  |> DataFrame ==  DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/oneline.jsonl", skip = 2, nrows = 10))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/oneline_plus.jsonl", skip = 2, nrows = 123))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/escapedeol.jsonl", skip = 5, nrows = 1))  |> DataFrame == DataFrame()

# skip = nrow(file) + nrows > 0
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/jsonlwebsite.jsonl", skip = nrow_fw, nrows = 10))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/mtcars.jsonl", skip = nrow_mt, nrows = 1))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/oneline.jsonl", skip = 1, nrows = 12)) |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/oneline_plus.jsonl", skip = 1, nrows = 1)) |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/escapedeol.jsonl", skip = 4, nrows = 1)) |> DataFrame == DataFrame()
end

@testset "skip" begin
# skip = nrow(file)
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/jsonlwebsite.jsonl", skip = nrow_fw))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/mtcars.jsonl", skip = nrow_mt))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/oneline.jsonl", skip = 1))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/oneline_plus.jsonl", skip = 1))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/escapedeol.jsonl", skip = 4))  |> DataFrame == DataFrame()

# skip > nrow(file)
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/jsonlwebsite.jsonl", skip = nrow_fw + 1))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/mtcars.jsonl", skip = nrow_mt + 42)) |> DataFrame  == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/mtcars.jsonl", skip = typemax(Int)))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/oneline.jsonl", skip = 2))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/oneline_plus.jsonl", skip = 2))  |> DataFrame == DataFrame()
    @test (@test_logs (:warn, "Skipped all lines") LineIndex("testfiles/escapedeol.jsonl", skip = 5)) |> DataFrame  == DataFrame()

# skip < nrow(file)
    @test LineIndex("testfiles/jsonlwebsite.jsonl", skip = nrow_fw - 1)  |> DataFrame == full_web[end:end, :]
    @test LineIndex("testfiles/mtcars.jsonl", skip = nrow_mt - 12)  |> DataFrame == noprom_mtcars[(end-11):end, :]
    @test LineIndex("testfiles/escapedeol.jsonl", skip = 2)  |> DataFrame == escaped[3:end, :]
end

@testset "nrows" begin
# nrows < nrow(file)
    @test LineIndex("testfiles/jsonlwebsite.jsonl", nrows = 2)  |> DataFrame == full_web[begin:2, :]
    @test LineIndex("testfiles/mtcars.jsonl", nrows = 12)  |> DataFrame == noprom_mtcars[begin:12, :]
    @test LineIndex("testfiles/escapedeol.jsonl", nrows = 3)  |> DataFrame == escaped[begin:3, :]

# nrows = nrow(file)
    @test LineIndex("testfiles/jsonlwebsite.jsonl", nrows = nrow_fw)  |> DataFrame == full_web
    @test LineIndex("testfiles/mtcars.jsonl", nrows = nrow_mt)  |> DataFrame == noprom_mtcars
    @test LineIndex("testfiles/oneline.jsonl", nrows = 1)  |> DataFrame == oneline
    @test LineIndex("testfiles/oneline_plus.jsonl", nrows = 1)  |> DataFrame == oneline_plus
    @test LineIndex("testfiles/escapedeol.jsonl", nrows = 4)  |> DataFrame == escaped

# nrows > nrow(file)
    @test LineIndex("testfiles/jsonlwebsite.jsonl", nrows = nrow_fw+1)  |> DataFrame == full_web
    @test LineIndex("testfiles/mtcars.jsonl", nrows = nrow_mt+100)  |> DataFrame == noprom_mtcars
    @test LineIndex("testfiles/oneline.jsonl", nrows = 2)  |> DataFrame == oneline
    @test LineIndex("testfiles/oneline_plus.jsonl", nrows = 2)  |> DataFrame == oneline_plus
    @test LineIndex("testfiles/escapedeol.jsonl", nrows = 5)  |> DataFrame == escaped
end

writelines("full_web.jsonl", full_web)
writelines("full_mtcars.jsonl", full_mtcars)
writelines("oneline2.jsonl", oneline)
writelines("oneline_plus2.jsonl", oneline_plus)
writelines("escaped2.jsonl", escaped)
@testset "write" begin
    @test LineIndex("full_web.jsonl") |> DataFrame == full_web
    @test LineIndex("full_mtcars.jsonl") |> DataFrame == full_mtcars
    @test LineIndex("oneline2.jsonl") |> DataFrame == oneline
    @test LineIndex("oneline_plus2.jsonl") |> DataFrame == oneline_plus
    @test LineIndex("escaped2.jsonl") |> DataFrame == escaped
end

@testset "MStructType" begin
    @MStructType MyType hp gear drat
    @test LineIndex("testfiles/mtcars.jsonl", structtype = MyType) |> DataFrame == full_mtcars[:, [:hp, :gear, :drat]]
    @MStructType EscType name
    @test LineIndex("testfiles/escapedeol.jsonl", structtype = EscType) |> DataFrame == escaped[:, [:name]]
end

@testset "Columnwise" begin
    mt_cur = LineIndex("testfiles/mtcars.jsonl")
    @test columnwise(mt_cur).mpg == noprom_mtcars.mpg
end

@testset "Filter" begin
    @test filter(row -> row[:mpg] > 20, LineIndex("testfiles/mtcars.jsonl")) |> DataFrame == filter(row -> row[:mpg] > 20, noprom_mtcars)
end

# Cleanup
rm("full_web.jsonl")
rm("full_mtcars.jsonl")
rm("oneline2.jsonl")
rm("oneline_plus2.jsonl")
rm("escaped2.jsonl")
