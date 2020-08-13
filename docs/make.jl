using JSONLines
using Documenter

makedocs(;
    modules=[JSONLines],
    authors="Daniel Winkler <danielw2904@disroot.org> and contributors",
    repo="https://github.com/danielw2904/JSONLines.jl/blob/{commit}{path}#L{line}",
    sitename="JSONLines.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://danielw2904.github.io/JSONLines.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/danielw2904/JSONLines.jl",
)
