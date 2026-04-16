namespace AITeam.CodeSight

open System
open System.Collections.Generic
open System.IO
open System.Text.RegularExpressions
open Jint
open AITeam.Sight.Core

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
    let create (index: CodeIndex) (chunks: CodeChunk[] option) (embeddingUrl: string) (indexDir: string) (repoRoot: string) (srcDirs: string[]) (peerPath: string option) =
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

        // Bridge primitives (L1): drift() and coverage()
        // Peer index is loaded lazily on first call
        let mutable peerIndex: Bridge.PeerTypes.KsPeerIndex option = None
        let getPeer () =
            match peerIndex with
            | Some p -> Some p
            | None ->
                match Bridge.PeerIndex.discover repoRoot peerPath with
                | Some ksDir ->
                    let p = Bridge.PeerIndex.load ksDir
                    peerIndex <- Some p
                    eprintfn "  Bridge: loaded KS peer index (%d chunks, %d source chunks)" p.Chunks.Length p.SourceChunks.Length
                    Some p
                | None -> None

        engine.SetValue("drift", Func<obj>(fun () ->
            match getPeer() with
            | Some peer -> box (stamp "drift" (Bridge.BridgePrimitives.drift index peer))
            | None -> box [| mdict [ "error", box "No KnowledgeSight index found at .knowledge-sight/ — run 'knowledge-sight index' first" ] |])) |> ignore

        engine.SetValue("coverage", Func<obj, obj>(fun opts ->
            let minFanIn = jsInt opts "minFanIn" 2
            match getPeer() with
            | Some peer -> box (stamp "coverage" (Bridge.BridgePrimitives.coverage index peer minFanIn))
            | None -> box [| mdict [ "error", box "No KnowledgeSight index found at .knowledge-sight/ — run 'knowledge-sight index' first" ] |])) |> ignore

        // Composition helpers + user-defined functions (from Sight.Core)
        QueryHelpers.registerHelpers engine Format.formatValue
        QueryHelpers.registerUserFunctions engine
            { FileName = "code-sight.functions.json"
              ReservedNames = FunctionStore.reservedNames }
            repoRoot

        engine

    /// Evaluate JS with IIFE wrapping — human-readable formatted output.
    let eval (engine: Engine) (js: string) : string =
        QueryHelpers.eval engine Format.formatValue js

    /// Evaluate JS with IIFE wrapping — raw JSON output for machine consumption.
    let evalJson (engine: Engine) (js: string) : string =
        QueryHelpers.evalJson engine js


