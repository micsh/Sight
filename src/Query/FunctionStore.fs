namespace AITeam.CodeSight

open System
open System.IO
open System.Text.Json
open System.Text.RegularExpressions

type FunctionDef = {
    Name: string
    Params: string[]
    Body: string
    Description: string
}

module FunctionStore =

    let private fileName = "code-sight.functions.json"

    let private filePath (repoRoot: string) = Path.Combine(repoRoot, fileName)

    /// Names that cannot be used as function names (primitives + JS reserved words).
    let reservedNames = set [
        "search"; "context"; "impact"; "imports"; "deps"; "expand"
        "neighborhood"; "similar"; "grep"; "files"; "modules"; "refs"; "walk"
        // JS reserved
        "var"; "let"; "const"; "function"; "return"; "if"; "else"; "for"; "while"
        "do"; "switch"; "case"; "break"; "continue"; "new"; "delete"; "typeof"
        "instanceof"; "in"; "of"; "try"; "catch"; "finally"; "throw"; "class"
        "import"; "export"; "default"; "this"; "super"; "yield"; "async"; "await"
        // Engine internals + composition helpers
        "__result__"; "pipe"; "tap"; "mergeBy"; "print"
    ]

    let private jsIdentifierRegex = Regex(@"^[a-zA-Z_$][a-zA-Z0-9_$]*$", RegexOptions.Compiled)

    let isValidIdentifier (name: string) =
        not (String.IsNullOrWhiteSpace name) && jsIdentifierRegex.IsMatch(name)

    let validateName (name: string) =
        if not (isValidIdentifier name) then
            Error $"'{name}' is not a valid identifier (use letters, digits, _ or $; must start with letter/_ /$)"
        elif reservedNames.Contains name then
            Error $"'{name}' is reserved (primitive or JS keyword)"
        else Ok name

    let validateParams (ps: string[]) =
        let bad = ps |> Array.tryFind (fun p -> not (isValidIdentifier p))
        match bad with
        | Some p -> Error $"Parameter '{p}' is not a valid identifier"
        | None -> Ok ps

    let load (repoRoot: string) : FunctionDef[] =
        let path = filePath repoRoot
        if not (File.Exists path) then [||]
        else
            try
                let json = File.ReadAllText(path)
                let doc = JsonDocument.Parse(json)
                doc.RootElement.EnumerateArray()
                |> Seq.map (fun (el: JsonElement) ->
                    let str (p: string) d = match el.TryGetProperty(p) with true, v -> v.GetString() | _ -> d
                    let arr (p: string) = match el.TryGetProperty(p) with true, v -> v.EnumerateArray() |> Seq.map (fun x -> x.GetString()) |> Seq.toArray | _ -> [||]
                    { Name = str "name" ""
                      Params = arr "params"
                      Body = str "body" ""
                      Description = str "description" "" })
                |> Seq.filter (fun f -> f.Name <> "" && f.Body <> "")
                |> Seq.toArray
            with ex ->
                eprintfn "Warning: failed to load %s: %s" path ex.Message
                [||]

    let save (repoRoot: string) (fns: FunctionDef[]) =
        let path = filePath repoRoot
        let options = JsonSerializerOptions(WriteIndented = true)
        let arr = fns |> Array.map (fun f ->
            dict [
                "name", box f.Name
                "params", box f.Params
                "body", box f.Body
                "description", box f.Description
            ])
        let json = JsonSerializer.Serialize(arr, options)
        File.WriteAllText(path, json)

    let add (repoRoot: string) (fn: FunctionDef) =
        match validateName fn.Name, validateParams fn.Params with
        | Error e, _ | _, Error e -> Error e
        | Ok _, Ok _ ->
            let existing = load repoRoot
            let updated = existing |> Array.filter (fun f -> f.Name <> fn.Name) |> Array.append [| fn |]
            save repoRoot updated
            let verb = if existing |> Array.exists (fun f -> f.Name = fn.Name) then "Updated" else "Added"
            Ok $"{verb} function '{fn.Name}'"

    let remove (repoRoot: string) (name: string) =
        let existing = load repoRoot
        if existing |> Array.exists (fun f -> f.Name = name) then
            let updated = existing |> Array.filter (fun f -> f.Name <> name)
            save repoRoot updated
            Ok $"Removed function '{name}'"
        else
            Error $"Function '{name}' not found"

    /// Generate JS function declarations for injection into the Jint engine.
    let toJsDeclarations (fns: FunctionDef[]) =
        fns |> Array.choose (fun f ->
            try
                let paramList = f.Params |> String.concat ", "
                let trimmed = f.Body.Trim()
                let bodyJs =
                    if trimmed.Contains("return ") then
                        trimmed
                    else
                        let lines = trimmed.Split('\n') |> Array.map (fun s -> s.Trim()) |> Array.filter (fun s -> s <> "")
                        let joined = lines |> String.concat " "
                        let lastSemi = joined.LastIndexOf(';')
                        if lastSemi > 0 && lastSemi < joined.Length - 2 then
                            let stmts = joined.Substring(0, lastSemi + 1)
                            let expr = joined.Substring(lastSemi + 1).Trim()
                            if expr.Length > 0 then $"{stmts} return {expr};"
                            else $"return {joined};"
                        else
                            $"return {joined};"
                Some $"function {f.Name}({paramList}) {{ {bodyJs} }}"
            with _ ->
                eprintfn "Warning: skipping malformed function '%s'" f.Name
                None)
