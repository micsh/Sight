namespace AITeam.Sight.Core

open System
open System.IO
open System.Net.Http
open System.Text
open System.Text.Json

/// HTTP client for embedding model (OpenAI-compatible /v1/embeddings).
module EmbeddingService =

    let private client = new HttpClient()

    let embed (url: string) (texts: string[]) = task {
        let body = JsonSerializer.Serialize {| input = texts |}
        use content = new StringContent(body, Encoding.UTF8, "application/json")
        try
            let! response = client.PostAsync(url, content)
            if not response.IsSuccessStatusCode then return None
            else
                let! json = response.Content.ReadAsStringAsync()
                let element = JsonElement.Parse(json)
                let data = element.GetProperty("data")
                let result =
                    data.EnumerateArray()
                    |> Seq.map (fun item ->
                        item.GetProperty("embedding").EnumerateArray()
                        |> Seq.map (fun x -> x.GetSingle()) |> Seq.toArray)
                    |> Seq.toArray
                return Some result
        with _ -> return None
    }

    /// Check if the embedding server is reachable.
    let probe (url: string) = task {
        try
            let! result = embed url [| "test" |]
            return result.IsSome
        with _ -> return false
    }
