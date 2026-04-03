namespace AITeam.CodeSight

open System
open System.Collections.Generic
open System.IO
open Jint

/// Jint-based query engine. Wires all 12 primitives, evaluates JS, formats results.
module QueryEngine =

    /// Create a Jint engine with all primitives wired.
    let create (index: CodeIndex) (chunks: CodeChunk[] option) (embeddingUrl: string) (indexDir: string) =
        let session = QuerySession(indexDir)
        let engine = Engine()

        // search
        engine.SetValue("search", Func<string, obj, obj>(fun query opts ->
            let limit, kind, file =
                match opts with
                | :? Jint.Native.JsObject as o ->
                    let l = match o.Get("limit") with v when not (v.IsUndefined()) -> int (v.AsNumber()) | _ -> 5
                    let k = match o.Get("kind") with v when not (v.IsUndefined()) && not (v.IsNull()) -> v.AsString() | _ -> ""
                    let f = match o.Get("file") with v when not (v.IsUndefined()) && not (v.IsNull()) -> v.AsString() | _ -> ""
                    l, k, f
                | _ -> 5, "", ""
            box (Primitives.search index session chunks embeddingUrl query limit kind file))) |> ignore

        // context
        engine.SetValue("context", Func<string, obj>(fun f -> box (Primitives.context index session f))) |> ignore

        // impact
        engine.SetValue("impact", Func<string, obj>(fun t -> box (Primitives.impact index session t))) |> ignore

        // imports
        engine.SetValue("imports", Func<string, obj>(fun f -> box (Primitives.imports index f))) |> ignore

        // deps
        engine.SetValue("deps", Func<string, obj>(fun p -> box (Primitives.deps index p))) |> ignore

        // expand
        engine.SetValue("expand", Func<string, obj>(fun id -> box (Primitives.expand index session chunks id))) |> ignore

        // neighborhood
        engine.SetValue("neighborhood", Func<string, obj, obj>(fun id opts ->
            let before, after =
                match opts with
                | :? Jint.Native.JsObject as o ->
                    let b = match o.Get("before") with v when not (v.IsUndefined()) -> int (v.AsNumber()) | _ -> 3
                    let a = match o.Get("after") with v when not (v.IsUndefined()) -> int (v.AsNumber()) | _ -> 3
                    b, a
                | _ -> 3, 3
            box (Primitives.neighborhood index session chunks id before after))) |> ignore

        // similar
        engine.SetValue("similar", Func<string, obj, obj>(fun id opts ->
            let limit =
                match opts with
                | :? Jint.Native.JsObject as o ->
                    match o.Get("limit") with v when not (v.IsUndefined()) -> int (v.AsNumber()) | _ -> 5
                | _ -> 5
            box (Primitives.similar index session id limit))) |> ignore

        // grep
        engine.SetValue("grep", Func<string, obj, obj>(fun pattern opts ->
            let limit, kind, file =
                match opts with
                | :? Jint.Native.JsObject as o ->
                    let l = match o.Get("limit") with v when not (v.IsUndefined()) -> int (v.AsNumber()) | _ -> 10
                    let k = match o.Get("kind") with v when not (v.IsUndefined()) && not (v.IsNull()) -> v.AsString() | _ -> ""
                    let f = match o.Get("file") with v when not (v.IsUndefined()) && not (v.IsNull()) -> v.AsString() | _ -> ""
                    l, k, f
                | _ -> 10, "", ""
            box (Primitives.grep index session chunks pattern limit kind file))) |> ignore

        // files
        engine.SetValue("files", Func<string, obj>(fun p -> box (Primitives.files index (if isNull p then "" else p)))) |> ignore

        // modules
        engine.SetValue("modules", Func<obj>(fun () -> box (Primitives.modules index))) |> ignore

        // refs
        engine.SetValue("refs", Func<string, obj, obj>(fun name opts ->
            let limit =
                match opts with
                | :? Jint.Native.JsObject as o ->
                    match o.Get("limit") with v when not (v.IsUndefined()) -> int (v.AsNumber()) | _ -> 20
                | _ -> 20
            box (Primitives.refs index session chunks name limit))) |> ignore

        // walk
        engine.SetValue("walk", Func<string, obj, obj>(fun name opts ->
            let depth, limit =
                match opts with
                | :? Jint.Native.JsObject as o ->
                    let d = match o.Get("depth") with v when not (v.IsUndefined()) -> int (v.AsNumber()) | _ -> 2
                    let l = match o.Get("limit") with v when not (v.IsUndefined()) -> int (v.AsNumber()) | _ -> 5
                    d, l
                | _ -> 2, 5
            box (Primitives.walk index session chunks name depth limit))) |> ignore

        engine

    /// Evaluate JS with IIFE wrapping (avoids let-redeclaration across calls).
    let eval (engine: Engine) (js: string) : string =
        try
            // Strip JS single-line comments before processing
            let stripped = js.Split('\n') |> Array.map (fun line ->
                let commentIdx = line.IndexOf("//")
                if commentIdx >= 0 then line.Substring(0, commentIdx) else line) |> String.concat "\n"
            let trimmed = stripped.Trim()
            // If already wrapped in IIFE, evaluate as-is
            let toEval =
                if trimmed.StartsWith("(") && trimmed.EndsWith(")") then trimmed
                else
                    let lines = trimmed.Split('\n') |> Array.map (fun s -> s.Trim()) |> Array.filter (fun s -> s <> "")
                    let joined = lines |> String.concat " "
                    let lastSemi = joined.LastIndexOf(';')
                    if lastSemi > 0 && lastSemi < joined.Length - 2 then
                        let stmts = joined.Substring(0, lastSemi + 1)
                        let expr = joined.Substring(lastSemi + 1).Trim()
                        if expr.Length > 0 then
                            sprintf "(function() { %s return %s; })()" stmts expr
                        else
                            sprintf "(function() { return %s; })()" joined
                    elif joined.StartsWith("let ") || joined.StartsWith("const ") || joined.StartsWith("var ") then
                        sprintf "(function() { %s })()" joined
                    else
                        sprintf "(function() { return %s; })()" joined

            // Evaluate JS, then stringify on the JS side to avoid .NET type boundary issues
            engine.SetValue("__result__", engine.Evaluate(toEval)) |> ignore
            let jsonResult = engine.Evaluate("typeof __result__ === 'string' ? __result__ : JSON.stringify(__result__, null, 2)")
            let text = jsonResult.AsString()

            // If result is a known primitive output (array of search results, etc.), apply rich formatting
            // Otherwise return the JSON as-is — LLMs read JSON fine
            if text.StartsWith("[R") || text.StartsWith("──") then
                // Already formatted by a primitive — pass through
                text
            else
                let native = engine.Evaluate("__result__").ToObject()
                try Format.formatValue native
                with _ -> text
        with ex -> sprintf "Error: %s" ex.Message


