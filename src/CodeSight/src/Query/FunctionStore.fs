namespace AITeam.CodeSight

open AITeam.Sight.Core

module FunctionStore =

    type FunctionDef = AITeam.Sight.Core.FunctionDef

    let private config = {
        FileName = "code-sight.functions.json"
        ReservedNames = FunctionStore.jsReservedNames + set [
            "search"; "context"; "impact"; "imports"; "deps"; "expand"
            "neighborhood"; "similar"; "grep"; "files"; "modules"; "refs"; "walk"
            "callers"; "changed"; "hotspots"; "explain"; "trace"; "arch"
            "drift"; "coverage"
        ]
    }

    let reservedNames = config.ReservedNames
    let validateName = FunctionStore.validateName config
    let load = FunctionStore.load config
    let save = FunctionStore.save config
    let add = FunctionStore.add config
    let remove = FunctionStore.remove config
    let toJsDeclarations = FunctionStore.toJsDeclarations
