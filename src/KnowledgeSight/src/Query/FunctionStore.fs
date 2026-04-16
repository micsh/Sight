namespace AITeam.KnowledgeSight

open AITeam.Sight.Core

module FunctionStore =

    type FunctionDef = AITeam.Sight.Core.FunctionDef

    let private config = {
        FileName = "knowledge-sight.functions.json"
        ReservedNames = FunctionStore.jsReservedNames + set [
            "catalog"; "search"; "context"; "expand"; "neighborhood"; "similar"
            "grep"; "mentions"; "files"; "backlinks"; "links"; "orphans"; "broken"
            "placement"; "walk"; "novelty"; "cluster"; "gaps"
            "changed"; "explain"
        ]
    }

    let reservedNames = config.ReservedNames
    let validateName = FunctionStore.validateName config
    let load = FunctionStore.load config
    let save = FunctionStore.save config
    let add = FunctionStore.add config
    let remove = FunctionStore.remove config
    let toJsDeclarations = FunctionStore.toJsDeclarations
