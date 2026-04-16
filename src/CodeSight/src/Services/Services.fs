namespace AITeam.CodeSight

open System
open System.IO
open System.Security.Cryptography
open System.Text.Json

/// File hashing for incremental indexing.
module FileHashing =

    /// Compute SHA256 hash of a file.
    let hashFile (path: string) =
        use fs = File.OpenRead(path)
        use sha = SHA256.Create()
        let bytes = sha.ComputeHash(fs)
        Convert.ToHexString(bytes).ToLowerInvariant()

    /// Load hashes from hashes.json.
    let loadHashes (path: string) : Map<string, string> =
        if not (File.Exists path) then Map.empty
        else
            let json = File.ReadAllText(path)
            let dict = JsonSerializer.Deserialize<Collections.Generic.Dictionary<string, string>>(json)
            dict |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq

    /// Save hashes to hashes.json.
    let saveHashes (path: string) (hashes: Map<string, string>) =
        let dict = Collections.Generic.Dictionary<string, string>()
        for kv in hashes do dict.[kv.Key] <- kv.Value
        let json = JsonSerializer.Serialize(dict, JsonSerializerOptions(WriteIndented = true))
        File.WriteAllText(path, json)

    /// Determine which files changed since last index.
    /// Returns (changedFiles, unchangedFiles, removedFiles).
    let diffFiles (currentFiles: string[]) (oldHashes: Map<string, string>) =
        let currentHashes = currentFiles |> Array.map (fun f -> f, hashFile f) |> Map.ofArray
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
