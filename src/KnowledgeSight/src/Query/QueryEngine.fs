namespace AITeam.KnowledgeSight

open System
open System.Collections.Generic
open System.IO
open System.Text.RegularExpressions
open Jint
open AITeam.Sight.Core

/// Jint-based query engine. Wires all primitives, evaluates JS, formats results.
module QueryEngine =

    let private sourceKey = "__ks_source__"

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

    /// Extract a float property from a JS options object.
    let private jsFloat (opts: obj) (key: string) (def: float) =
        match opts with
        | :? Jint.Native.JsValue as v when v.IsObject() ->
            let o = v.AsObject()
            let prop = o.Get(key)
            if prop.IsUndefined() || prop.IsNull() then def else prop.AsNumber()
        | :? System.Dynamic.ExpandoObject as eo ->
            let dict = eo :> IDictionary<string, obj>
            match dict.TryGetValue(key) with
            | true, v when not (isNull v) -> System.Convert.ToDouble(v)
            | _ -> def
        | _ -> def

    /// Stamp each result item with its source primitive name for format disambiguation.
    let private stamp (source: string) (results: Dictionary<string, obj>[]) =
        for d in results do d.[sourceKey] <- box source
        results

    let private stamp1 (source: string) (result: Dictionary<string, obj>) =
        result.[sourceKey] <- box source
        result

    let create (index: DocIndex) (chunks: DocChunk[] option) (embeddingUrl: string) (indexDir: string) (repoRoot: string) =
        let session = QuerySession(indexDir)
        let engine = new Engine()

        // catalog
        engine.SetValue("catalog", Func<obj>(fun () -> box (stamp "catalog" (Primitives.catalog index)))) |> ignore

        // search
        engine.SetValue("search", Func<string, obj, obj>(fun query opts ->
            let limit = jsInt opts "limit" 5
            let tag = jsStr opts "tag" ""
            let file = jsStr opts "file" ""
            box (stamp "search" (Primitives.search index session chunks embeddingUrl query limit tag file)))) |> ignore

        // context
        engine.SetValue("context", Func<string, obj>(fun f -> box (stamp1 "context" (Primitives.context index session f)))) |> ignore

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
            let file = jsStr opts "file" ""
            box (stamp "grep" (Primitives.grep index session chunks pattern limit file)))) |> ignore

        // mentions
        engine.SetValue("mentions", Func<string, obj, obj>(fun term opts ->
            let limit = jsInt opts "limit" 20
            box (stamp "mentions" (Primitives.mentions index session chunks term limit)))) |> ignore

        // files
        engine.SetValue("files", Func<string, obj>(fun p -> box (stamp "files" (Primitives.files index (if isNull p then "" else p))))) |> ignore

        // backlinks
        engine.SetValue("backlinks", Func<string, obj>(fun f -> box (stamp "backlinks" (Primitives.backlinks index session f)))) |> ignore

        // links
        engine.SetValue("links", Func<string, obj>(fun f -> box (stamp "links" (Primitives.links index f)))) |> ignore

        // orphans
        engine.SetValue("orphans", Func<obj>(fun () -> box (stamp "orphans" (Primitives.orphans index)))) |> ignore

        // broken
        engine.SetValue("broken", Func<obj>(fun () -> box (stamp "broken" (Primitives.broken index)))) |> ignore

        // placement
        engine.SetValue("placement", Func<string, obj, obj>(fun content opts ->
            let limit = jsInt opts "limit" 3
            box (stamp "placement" (Primitives.placement index embeddingUrl content limit)))) |> ignore

        // walk
        engine.SetValue("walk", Func<string, obj, obj>(fun file opts ->
            let depth = jsInt opts "depth" 2
            let direction = jsStr opts "direction" "out"
            box (stamp "walk" (Primitives.walk index session file depth direction)))) |> ignore

        // novelty
        engine.SetValue("novelty", Func<string, obj, obj>(fun text opts ->
            let threshold = jsFloat opts "threshold" 0.75
            box (stamp "novelty" (Primitives.novelty index embeddingUrl text threshold)))) |> ignore

        // cluster
        engine.SetValue("cluster", Func<string, obj, obj>(fun dir opts ->
            let threshold = jsFloat opts "threshold" 0.7
            box (stamp "cluster" (Primitives.cluster index dir threshold)))) |> ignore

        // gaps — use JsValue to avoid Jint's ToObject() conversion
        engine.SetValue("gaps", Func<Jint.Native.JsValue, obj>(fun opts ->
            let scope, minDocs, signal =
                if isNull (box opts) || opts.IsUndefined() || opts.IsNull() then "", 1, ""
                elif opts.IsString() then opts.AsString(), 1, ""
                elif opts.IsObject() then
                    let o = opts.AsObject()
                    let s = match o.Get("scope") with v when not (v.IsUndefined()) && not (v.IsNull()) -> v.AsString() | _ -> ""
                    let m = match o.Get("min_docs") with v when not (v.IsUndefined()) -> int (v.AsNumber()) | _ -> 1
                    let sig' = match o.Get("signal") with v when not (v.IsUndefined()) && not (v.IsNull()) -> v.AsString() | _ -> ""
                    s, m, sig'
                else "", 1, ""
            box (stamp "gaps" (Primitives.gaps index chunks scope minDocs signal)))) |> ignore

        // changed
        engine.SetValue("changed", Func<string, obj>(fun gitRef ->
            box (stamp "changed" (Primitives.changed index session repoRoot gitRef)))) |> ignore

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

        // Composition helpers + user-defined functions (from Sight.Core)
        QueryHelpers.registerHelpers engine Format.formatValue
        QueryHelpers.registerUserFunctions engine
            { FileName = "knowledge-sight.functions.json"
              ReservedNames = FunctionStore.reservedNames }
            repoRoot

        engine

    /// Wrap JS in an IIFE for evaluation.
    /// Evaluate JS with IIFE wrapping — human-readable formatted output.
    let eval (engine: Engine) (js: string) : string =
        QueryHelpers.eval engine Format.formatValue js

    /// Evaluate JS with IIFE wrapping — raw JSON output for machine consumption.
    let evalJson (engine: Engine) (js: string) : string =
        QueryHelpers.evalJson engine js
