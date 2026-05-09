namespace AITeam.Sight.Core

open System
open System.Text.RegularExpressions
open Jint

/// Shared query engine helpers — IIFE wrapping, ref-ID rewriting, composition helpers.
module QueryHelpers =

    /// JS source for pipe/tap/mergeBy composition helpers.
    let compositionHelpersJs = """
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
    """

    /// Register composition helpers (pipe/tap/mergeBy) and print on a Jint engine.
    /// `formatValue` is the tool-specific formatter for print output.
    let registerHelpers (engine: Engine) (formatValue: obj -> string) =
        engine.SetValue("print", Action<obj>(fun v ->
            eprintfn "%s" (formatValue v))) |> ignore
        engine.Execute(compositionHelpersJs) |> ignore

    /// Load and register user-defined functions on a Jint engine.
    let registerUserFunctions (engine: Engine) (config: FunctionStoreConfig) (repoRoot: string) =
        match FunctionStore.load config repoRoot with
        | Error msg ->
            eprintfn "Warning: %s" msg
        | Ok userFns ->
            for fn in userFns do
                match FunctionJsAdapter.tryRenderDeclaration fn with
                | Error msg ->
                    eprintfn "Warning: %s" msg
                | Ok decl ->
                    try engine.Execute(decl) |> ignore
                    with ex -> eprintfn "Warning: failed to load function '%s': %s" fn.Name ex.Message

    /// Rewrite bare R123 tokens to 'R123' string literals (outside of quotes).
    let rewriteRefIds (js: string) =
        Regex.Replace(js, @"(?<![""'a-zA-Z_])R(\d+)(?![""'a-zA-Z_])", "'R$1'")

    let private tryFindLastTopLevelSemicolon (js: string) =
        let mutable lastSemi = -1
        let mutable inSingleQuoted = false
        let mutable inDoubleQuoted = false
        let mutable inTemplateQuoted = false
        let mutable escapeNext = false
        let mutable parenDepth = 0
        let mutable bracketDepth = 0
        let mutable braceDepth = 0

        for i in 0 .. js.Length - 1 do
            let ch = js.[i]

            if escapeNext then
                escapeNext <- false
            elif inSingleQuoted then
                if ch = '\\' then escapeNext <- true
                elif ch = '\'' then inSingleQuoted <- false
            elif inDoubleQuoted then
                if ch = '\\' then escapeNext <- true
                elif ch = '"' then inDoubleQuoted <- false
            elif inTemplateQuoted then
                if ch = '\\' then escapeNext <- true
                elif ch = '`' then inTemplateQuoted <- false
            else
                match ch with
                | '\'' -> inSingleQuoted <- true
                | '"' -> inDoubleQuoted <- true
                | '`' -> inTemplateQuoted <- true
                | '(' -> parenDepth <- parenDepth + 1
                | ')' when parenDepth > 0 -> parenDepth <- parenDepth - 1
                | '[' -> bracketDepth <- bracketDepth + 1
                | ']' when bracketDepth > 0 -> bracketDepth <- bracketDepth - 1
                | '{' -> braceDepth <- braceDepth + 1
                | '}' when braceDepth > 0 -> braceDepth <- braceDepth - 1
                | ';' when parenDepth = 0 && bracketDepth = 0 && braceDepth = 0 -> lastSemi <- i
                | _ -> ()

        if lastSemi >= 0 then Some lastSemi else None

    /// Wrap JS in an IIFE for evaluation.
    let wrapIIFE (js: string) =
        let refRewritten = rewriteRefIds js
        let stripped = refRewritten.Split('\n') |> Array.map (fun line ->
            let commentIdx = line.IndexOf("//")
            if commentIdx >= 0 then line.Substring(0, commentIdx) else line) |> String.concat "\n"
        let trimmed = stripped.Trim()
        if trimmed.StartsWith("(") && trimmed.EndsWith(")") then trimmed
        else
            let lines = trimmed.Split('\n') |> Array.map (fun s -> s.Trim()) |> Array.filter (fun s -> s <> "")
            let joined = lines |> String.concat " "
            match tryFindLastTopLevelSemicolon joined with
            | Some lastSemi when lastSemi > 0 && lastSemi < joined.Length - 2 ->
                let stmts = joined.Substring(0, lastSemi + 1)
                let expr = joined.Substring(lastSemi + 1).Trim()
                if expr.Length > 0 then
                    sprintf "(function() { %s return %s; })()" stmts expr
                else
                    sprintf "(function() { return %s; })()" joined
            | _ when joined.StartsWith("let ") || joined.StartsWith("const ") || joined.StartsWith("var ") ->
                sprintf "(function() { %s })()" joined
            | _ ->
                sprintf "(function() { return %s; })()" joined

    /// Evaluate JS with IIFE wrapping — human-readable formatted output.
    /// `formatValue` is the tool-specific formatter.
    let eval (engine: Engine) (formatValue: obj -> string) (js: string) : string =
        try
            let toEval = wrapIIFE js
            engine.SetValue("__result__", engine.Evaluate(toEval)) |> ignore
            let jsonResult = engine.Evaluate("typeof __result__ === 'string' ? __result__ : JSON.stringify(__result__, null, 2)")
            let text = jsonResult.AsString()

            if text.StartsWith("[R") || text.StartsWith("──") then text
            else
                let native = engine.Evaluate("__result__").ToObject()
                try formatValue native
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
