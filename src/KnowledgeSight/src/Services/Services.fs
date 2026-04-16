namespace AITeam.KnowledgeSight

open System
open System.IO
open System.Security.Cryptography
open System.Text.Json

/// File hashing for incremental indexing.
module FileHashing =

    let hashFile (path: string) =
        if not (File.Exists path) then ""
        else
            use fs = File.OpenRead(path)
            use sha = SHA256.Create()
            let bytes = sha.ComputeHash(fs)
            Convert.ToHexString(bytes).ToLowerInvariant()

    let loadHashes (path: string) : Map<string, string> =
        if not (File.Exists path) then Map.empty
        else
            try
                let json = File.ReadAllText(path)
                let dict = JsonSerializer.Deserialize<Collections.Generic.Dictionary<string, string>>(json)
                dict |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq
            with _ -> Map.empty

    let saveHashes (path: string) (hashes: Map<string, string>) =
        let dict = Collections.Generic.Dictionary<string, string>()
        for kv in hashes do dict.[kv.Key] <- kv.Value
        let json = JsonSerializer.Serialize(dict, JsonSerializerOptions(WriteIndented = true))
        File.WriteAllText(path, json)

    let diffFiles (currentFiles: string[]) (oldHashes: Map<string, string>) (repoRoot: string) =
        let currentHashes = currentFiles |> Array.map (fun f -> f, hashFile (Path.Combine(repoRoot, f))) |> Array.filter (fun (_, h) -> h <> "") |> Map.ofArray
        let changed =
            currentHashes |> Map.toArray |> Array.filter (fun (f, h) ->
                match Map.tryFind f oldHashes with
                | Some oldH -> oldH <> h
                | None -> true)
            |> Array.map fst
        let removed =
            oldHashes |> Map.toArray |> Array.filter (fun (f, _) ->
                not (currentHashes.ContainsKey f))
            |> Array.map fst
        let unchanged =
            currentHashes |> Map.toArray |> Array.filter (fun (f, h) ->
                match Map.tryFind f oldHashes with
                | Some oldH -> oldH = h
                | None -> false)
            |> Array.map fst
        changed, unchanged, removed, currentHashes
