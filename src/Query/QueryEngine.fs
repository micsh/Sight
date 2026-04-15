namespace AITeam.CodeSight

open System
open System.Collections.Generic
open System.IO
open System.Text.RegularExpressions
open Jint

/// Jint-based query engine. Wires all 12 primitives, evaluates JS, formats results.
module QueryEngine =

    let private sourceKey = "__cs_source__"

    /// Extract a string property from a JS options object, handling JsObject / ObjectInstance / JsValue.
    let private jsStr (opts: obj) (key: string) (def: string) =
        match opts with
        | :? Jint.Native.JsValue as v when v.IsObject() ->
            let o = v.AsObject()
            let prop = o.Get(key)
            if prop.IsUndefined() || prop.IsNull() then def else prop.AsString()
        | :? System.Dynamic.ExpandoObject as eo ->
            let dict = eo :> IDictionary<string, obj>
            match dict.TryGetValue(key) with
            | true, v when not (isNull v) -> string v
            | _ -> def
        | _ -> def

    /// Extract an int property from a JS options object.
    let private jsInt (opts: obj) (key: string) (def: int) =
        match opts with
        | :? Jint.Native.JsValue as v when v.IsObject() ->
            let o = v.AsObject()
            let prop = o.Get(key)
            if prop.IsUndefined() || prop.IsNull() then def else int (prop.AsNumber())
        | :? System.Dynamic.ExpandoObject as eo ->
            let dict = eo :> IDictionary<string, obj>
            match dict.TryGetValue(key) with
            | true, v when not (isNull v) -> int (System.Convert.ToDouble(v))
            | _ -> def
        | _ -> def

    let private stamp (source: string) (results: Dictionary<string, obj>[]) =
        for d in results do d.[sourceKey] <- box source
        results

    let private stamp1 (source: string) (result: Dictionary<string, obj>) =
        result.[sourceKey] <- box source
        result

    /// Create a Jint engine with all primitives wired.
    let create (index: CodeIndex) (chunks: CodeChunk[] option) (embeddingUrl: string) (indexDir: string) (repoRoot: string) (srcDirs: string[]) =
        let session = QuerySession(indexDir)
        let engine = Engine()

        // search
        engine.SetValue("search", Func<string, obj, obj>(fun query opts ->
            let limit = jsInt opts "limit" 5
            let kind = jsStr opts "kind" ""
            let file = jsStr opts "file" ""
            box (stamp "search" (Primitives.search index session chunks embeddingUrl query limit kind file)))) |> ignore

        // context
        engine.SetValue("context", Func<string, obj>(fun f -> box (stamp1 "context" (Primitives.context index session f)))) |> ignore

        // impact
        engine.SetValue("impact", Func<string, obj>(fun t -> box (stamp "impact" (Primitives.impact index session t)))) |> ignore

        // imports (returns string[], not dicts)
        engine.SetValue("imports", Func<string, obj>(fun f -> box (Primitives.imports index f))) |> ignore

        // deps (returns string[], not dicts)
        engine.SetValue("deps", Func<string, obj>(fun p -> box (Primitives.deps index p))) |> ignore

        // expand
        engine.SetValue("expand", Func<string, obj>(fun id -> box (stamp1 "expand" (Primitives.expand index session chunks id)))) |> ignore

        // neighborhood
        engine.SetValue("neighborhood", Func<string, obj, obj>(fun id opts ->
            let before = jsInt opts "before" 3
            let after = jsInt opts "after" 3
            box (stamp1 "neighborhood" (Primitives.neighborhood index session chunks id before after)))) |> ignore

        // similar
        engine.SetValue("similar", Func<string, obj, obj>(fun id opts ->
            let limit = jsInt opts "limit" 5
            box (stamp "similar" (Primitives.similar index session id limit)))) |> ignore

        // grep
        engine.SetValue("grep", Func<string, obj, obj>(fun pattern opts ->
            let limit = jsInt opts "limit" 10
            let kind = jsStr opts "kind" ""
            let file = jsStr opts "file" ""
            box (stamp "grep" (Primitives.grep index session chunks pattern limit kind file)))) |> ignore

        // files
        engine.SetValue("files", Func<string, obj>(fun p -> box (stamp "files" (Primitives.files index (if isNull p then "" else p))))) |> ignore

        // modules
        engine.SetValue("modules", Func<obj>(fun () -> box (stamp "modules" (Primitives.modules index srcDirs)))) |> ignore

        // refs
        engine.SetValue("refs", Func<string, obj, obj>(fun name opts ->
            let limit = jsInt opts "limit" 20
            box (stamp "refs" (Primitives.refs index session chunks name limit)))) |> ignore

        // walk
        engine.SetValue("walk", Func<string, obj, obj>(fun name opts ->
            let depth = jsInt opts "depth" 2
            let limit = jsInt opts "limit" 5
            box (stamp "walk" (Primitives.walk index session chunks name depth limit)))) |> ignore

        // callers
        engine.SetValue("callers", Func<string, obj, obj>(fun name opts ->
            let limit = jsInt opts "limit" 20
            box (stamp "callers" (Primitives.callers index session chunks name limit)))) |> ignore

        // changed
        engine.SetValue("changed", Func<string, obj>(fun gitRef ->
            box (stamp "changed" (Primitives.changed index session repoRoot gitRef)))) |> ignore

        // hotspots
        engine.SetValue("hotspots", Func<obj, obj>(fun opts ->
            let sortBy = jsStr opts "by" "chunks"
            let minChunks = jsInt opts "min" 1
            box (stamp "hotspots" (Primitives.hotspots index sortBy minChunks)))) |> ignore

        // explain
        engine.SetValue("explain", Func<string, obj>(fun refId ->
            box (stamp1 "explain" (Primitives.explain index session chunks refId)))) |> ignore

        // session save/load/list
        engine.SetValue("saveSession", Func<string, obj>(fun name ->
            session.SaveSession(name)
            box (mdict [ "saved", box name; "refs", box session.RefCount ]))) |> ignore
        engine.SetValue("loadSession", Func<string, obj>(fun name ->
            if session.LoadSession(name) then
                box (mdict [ "loaded", box name; "refs", box session.RefCount ])
            else
                box (mdict [ "error", box (sprintf "session '%s' not found" name) ]))) |> ignore
        engine.SetValue("sessions", Func<obj>(fun () ->
            box (session.ListSessions()))) |> ignore

        // trace
        engine.SetValue("trace", Func<string, string, obj>(fun fromName toName ->
            box (stamp "trace" (Primitives.trace index fromName toName)))) |> ignore

        // arch
        engine.SetValue("arch", Func<string, obj>(fun filePath ->
            box (stamp1 "arch" (Primitives.arch repoRoot filePath)))) |> ignore

        // Composition helpers
        engine.SetValue("print", Action<obj>(fun v ->
            eprintfn "%s" (Format.formatValue v))) |> ignore

        engine.Execute("""
            function pipe(value) {
                var fns = Array.prototype.slice.call(arguments, 1);
                return fns.reduce(function(acc, fn) { return fn(acc); }, value);
            }
            function tap(value, fn) { fn(value); return value; }
            function mergeBy(key) {
                var arrays = Array.prototype.slice.call(arguments, 1);
                var seen = {};
                var result = [];
                for (var i = 0; i < arrays.length; i++) {
                    var arr = arrays[i];
                    if (!arr) continue;
                    for (var j = 0; j < arr.length; j++) {
                        var item = arr[j];
                        var k = item[key];
                        if (k !== undefined && !seen[k]) { seen[k] = true; result.push(item); }
                        else if (k === undefined) { result.push(item); }
                    }
                }
                return result;
            }
        """) |> ignore

        // Load user-defined functions
        let userFns = FunctionStore.load repoRoot
        let fnDecls = FunctionStore.toJsDeclarations userFns
        for decl in fnDecls do
            try engine.Execute(decl) |> ignore
            with ex -> eprintfn "Warning: failed to load function: %s" ex.Message

        engine

    /// Wrap JS in an IIFE for evaluation.
    let private wrapIIFE (js: string) =
        // Ref-ID shorthand: rewrite bare R123 to 'R123' (outside of strings)
        let refRewritten = Regex.Replace(js, @"(?<![""'a-zA-Z_])R(\d+)(?![""'a-zA-Z_])", "'R$1'")
        let stripped = refRewritten.Split('\n') |> Array.map (fun line ->
            let commentIdx = line.IndexOf("//")
            if commentIdx >= 0 then line.Substring(0, commentIdx) else line) |> String.concat "\n"
        let trimmed = stripped.Trim()
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

    /// Evaluate JS with IIFE wrapping — human-readable formatted output.
    let eval (engine: Engine) (js: string) : string =
        try
            let toEval = wrapIIFE js
            engine.SetValue("__result__", engine.Evaluate(toEval)) |> ignore
            let jsonResult = engine.Evaluate("typeof __result__ === 'string' ? __result__ : JSON.stringify(__result__, null, 2)")
            let text = jsonResult.AsString()

            if text.StartsWith("[R") || text.StartsWith("──") then text
            else
                let native = engine.Evaluate("__result__").ToObject()
                try Format.formatValue native
                with _ -> text
        with ex -> sprintf "Error: %s" ex.Message

    /// Evaluate JS with IIFE wrapping — raw JSON output for machine consumption.
    let evalJson (engine: Engine) (js: string) : string =
        try
            let toEval = wrapIIFE js
            engine.SetValue("__result__", engine.Evaluate(toEval)) |> ignore
            let jsonResult = engine.Evaluate("typeof __result__ === 'string' ? __result__ : JSON.stringify(__result__, null, 2)")
            jsonResult.AsString()
        with ex ->
            let escaped = ex.Message.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", " ")
            sprintf """{"error":"%s"}""" escaped


