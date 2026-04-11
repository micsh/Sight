namespace AITeam.CodeSight

open System.Collections.Generic
open System.Dynamic
open System.Text.Json

/// Result formatting for LLM consumption.
module Format =

    let private sourceKey = "__cs_source__"

    let private fmtScore (d: IDictionary<string, obj>) =
        match d.TryGetValue("score") with
        | true, v ->
            match v with
            | :? float as f -> sprintf "%.2f  " f
            | _ -> try sprintf "%.2f  " (System.Convert.ToDouble(v)) with _ -> ""
        | _ -> ""

    /// Convert ExpandoObject (and nested ones) to IDictionary for uniform handling.
    let rec private normalize (v: obj) : obj =
        match v with
        | :? ExpandoObject as expando ->
            let d = Dictionary<string, obj>()
            for kv in (expando :> IDictionary<string, obj>) do
                d.[kv.Key] <- normalize kv.Value
            box d
        | :? (obj[]) as arr ->
            box (arr |> Array.map normalize)
        | _ -> v

    let rec formatValue (v: obj) : string =
        let v = normalize v
        match v with
        | :? (IDictionary<string, obj>) as d ->
            let lines = ResizeArray<string>()
            match d.TryGetValue("error") with
            | true, err -> lines.Add(sprintf "Error: %O" err)
            | _ ->
                // Neighborhood format
                match d.TryGetValue("target"), d.TryGetValue("before"), d.TryGetValue("after") with
                | (true, _), (true, _), (true, _) ->
                    let file = match d.TryGetValue("file") with true, v -> string v | _ -> ""
                    lines.Add(sprintf "── %s ──" file)
                    match d.TryGetValue("imports") with
                    | true, (:? (string[]) as imps) when imps.Length > 0 -> lines.Add(sprintf "Imports: %s" (imps |> String.concat ", "))
                    | _ -> ()
                    lines.Add("")
                    let fmtChunk (c: obj) =
                        match c with
                        | :? IDictionary<string, obj> as cd ->
                            let cid = match cd.TryGetValue("id") with true, v -> string v | _ -> ""
                            let ckind = match cd.TryGetValue("kind") with true, v -> string v | _ -> ""
                            let cname = match cd.TryGetValue("name") with true, v -> string v | _ -> ""
                            let cline = match cd.TryGetValue("line") with true, v -> string v | _ -> ""
                            let csig = match cd.TryGetValue("signature") with true, v when string v <> "" -> sprintf "  %s" (string v) | _ -> ""
                            let csum = match cd.TryGetValue("summary") with true, v -> string v | _ -> ""
                            let cprev = match cd.TryGetValue("preview") with true, v when string v <> "" -> sprintf "\n         ▸ %s" (string v) | _ -> ""
                            sprintf "  [%s] %s:%s (L%s)%s — %s%s" cid ckind cname cline csig csum cprev
                        | _ -> string c
                    match d.TryGetValue("before") with
                    | true, (:? (obj[]) as arr) -> for item in arr do lines.Add(fmtChunk item)
                    | _ -> ()
                    lines.Add("  ─── ▼ target ▼ ───")
                    match d.TryGetValue("target") with
                    | true, (:? IDictionary<string, obj> as td) ->
                        let tid = match td.TryGetValue("id") with true, v -> string v | _ -> ""
                        let tkind = match td.TryGetValue("kind") with true, v -> string v | _ -> ""
                        let tname = match td.TryGetValue("name") with true, v -> string v | _ -> ""
                        let tline = match td.TryGetValue("line") with true, v -> string v | _ -> ""
                        let tsig = match td.TryGetValue("signature") with true, v when string v <> "" -> sprintf "  %s" (string v) | _ -> ""
                        let tsum = match td.TryGetValue("summary") with true, v -> string v | _ -> ""
                        let tcode = match td.TryGetValue("code") with true, v -> string v | _ -> ""
                        lines.Add(sprintf "  [%s] %s:%s (L%s)%s — %s" tid tkind tname tline tsig tsum)
                        if tcode <> "" then lines.Add(sprintf "  ```\n%s\n  ```" tcode)
                    | _ -> ()
                    lines.Add("  ─── ▲ target ▲ ───")
                    match d.TryGetValue("after") with
                    | true, (:? (obj[]) as arr) -> for item in arr do lines.Add(fmtChunk item)
                    | _ -> ()
                | _ ->

                let id = match d.TryGetValue("id") with true, v -> string v | _ -> ""
                let kind = match d.TryGetValue("kind") with true, v -> string v | _ -> ""
                let name = match d.TryGetValue("name") with true, v -> string v | _ -> ""
                let file = match d.TryGetValue("file") with true, v -> string v | _ -> ""
                let line = match d.TryGetValue("line") with true, v -> string v | _ -> ""
                let score = fmtScore d
                let summary = match d.TryGetValue("summary") with true, v -> string v | _ -> ""
                let sigStr = match d.TryGetValue("signature") with true, v when string v <> "" -> sprintf "\n       %s" (string v) | _ -> ""
                let code = match d.TryGetValue("code") with true, v -> string v | _ -> ""
                let matchLine = match d.TryGetValue("matchLine") with true, v when string v <> "" -> sprintf "\n       ▸ %s" (string v) | _ -> ""
                let preview = match d.TryGetValue("preview") with true, v when string v <> "" -> sprintf "\n       ▸ %s" (string v) | _ -> ""

                if code <> "" then
                    lines.Add(sprintf "[%s] %s:%s (%s:%s)%s" id kind name file line sigStr)
                    lines.Add(sprintf "```\n%s\n```" code)
                elif id <> "" then
                    lines.Add(sprintf "[%s] %s%s:%s (%s:%s)%s\n       %s%s%s" id score kind name file line sigStr summary matchLine preview)
                else
                    for kv in d do
                        if kv.Key <> sourceKey then
                            let valStr =
                                try
                                    let f = System.Convert.ToDouble(kv.Value)
                                    sprintf "%g" f
                                with _ -> formatValue kv.Value
                            lines.Add(sprintf "%s: %s" kv.Key valStr)

                match d.TryGetValue("imports") with | true, (:? (string[]) as imps) when imps.Length > 0 -> lines.Add(sprintf "Imports: %s" (imps |> String.concat ", ")) | _ -> ()
                match d.TryGetValue("dependents") with | true, (:? (string[]) as deps) when deps.Length > 0 -> lines.Add(sprintf "Dependents: %s" (deps |> String.concat ", ")) | _ -> ()
                match d.TryGetValue("chunks") with
                | true, chunksVal ->
                    match chunksVal with
                    | :? (obj[]) as arr ->
                        for item in arr do
                            match item with
                            | :? IDictionary<string, obj> as c ->
                                let cid = match c.TryGetValue("id") with true, v -> string v | _ -> ""
                                let ckind = match c.TryGetValue("kind") with true, v -> string v | _ -> ""
                                let cname = match c.TryGetValue("name") with true, v -> string v | _ -> ""
                                let cline = match c.TryGetValue("line") with true, v -> string v | _ -> ""
                                let csig = match c.TryGetValue("signature") with true, v when string v <> "" -> sprintf "  %s" (string v) | _ -> ""
                                let csum = match c.TryGetValue("summary") with true, v -> string v | _ -> ""
                                lines.Add(sprintf "[%s] %s:%s (L%s)%s — %s" cid ckind cname cline csig csum)
                            | _ -> ()
                    | _ -> ()
                | _ -> ()
            lines |> String.concat "\n"

        | :? (obj[]) as arr when arr.Length > 0 && (arr.[0] :? IDictionary<string, obj>) ->
            arr |> Array.map (fun item ->
                let d = item :?> IDictionary<string, obj>
                match d.TryGetValue("module") with
                | true, m ->
                    let files = match d.TryGetValue("files") with true, v -> string v | _ -> "?"
                    let chunks = match d.TryGetValue("chunks") with true, v -> string v | _ -> "?"
                    let topTypes = match d.TryGetValue("topTypes") with true, v when string v <> "" -> sprintf "\n    Types: %s" (string v) | _ -> ""
                    let summaries = match d.TryGetValue("summaries") with true, v when string v <> "" -> sprintf "\n    %s" (string v) | _ -> ""
                    sprintf "%-30s %s files, %s chunks%s%s" (string m) files chunks topTypes summaries
                | _ ->
                let id = match d.TryGetValue("id") with true, v -> string v | _ -> ""
                let kind = match d.TryGetValue("kind") with true, v -> string v | _ -> ""
                let name = match d.TryGetValue("name") with true, v -> string v | _ -> ""
                let file = match d.TryGetValue("file") with true, v -> string v | _ -> ""
                let line = match d.TryGetValue("line") with true, v -> string v | _ -> ""
                let score = fmtScore d
                let summary = match d.TryGetValue("summary") with true, v -> string v | _ -> ""
                let sigStr = match d.TryGetValue("signature") with true, v when string v <> "" -> sprintf "\n       %s" (string v) | _ -> ""
                let matchLine = match d.TryGetValue("matchLine") with true, v when string v <> "" -> sprintf "\n       ▸ %s" (string v) | _ -> ""
                let preview = match d.TryGetValue("preview") with true, v when string v <> "" -> sprintf "\n       ▸ %s" (string v) | _ -> ""
                if id <> "" then sprintf "[%s] %s%s:%s (%s:%s)%s\n       %s%s%s" id score kind name file line sigStr summary matchLine preview
                elif file <> "" then sprintf "%s — %s" file summary
                else
                    d |> Seq.filter (fun kv -> kv.Key <> sourceKey)
                    |> Seq.map (fun kv -> sprintf "%s=%s" kv.Key (formatValue kv.Value)) |> String.concat ", " |> sprintf "{%s}")
            |> String.concat "\n"

        | :? (obj[]) as arr when arr.Length > 0 && (arr.[0] :? string) -> arr |> Array.map string |> String.concat "\n"
        | :? (string[]) as arr -> arr |> String.concat "\n"
        | :? (obj[]) as arr when arr.Length = 0 -> "(no results)"
        | null -> "(no results)"
        | :? string as s -> s
        | :? int as i -> string i
        | :? float as f -> if f = System.Math.Floor(f) && abs f < 1e15 then sprintf "%.0f" f else sprintf "%.3f" f
        | :? bool as b -> if b then "true" else "false"
        | other ->
            // Fallback: try JSON serialization for unknown types
            try JsonSerializer.Serialize(other, JsonSerializerOptions(WriteIndented = true))
            with _ -> string other

