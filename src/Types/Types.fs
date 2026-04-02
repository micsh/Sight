namespace AITeam.CodeSight

/// A chunk of source code extracted by tree-sitter parsing.
type CodeChunk = {
    FilePath: string
    Module: string
    Name: string
    Kind: string        // "module" | "type" | "let" | "member" | "du" | "record"
    StartLine: int
    EndLine: int
    Content: string
    Context: string     // "namespace X\nmodule Y\ntype Z" prefix for embedding
}

/// A chunk entry in the persisted index. Core fields + dynamic extras.
type ChunkEntry = {
    FilePath: string
    Module: string
    Name: string
    Kind: string
    StartLine: int
    EndLine: int
    Summary: string
    Signature: string
    Extra: Map<string, string>
}

/// The in-memory code index. All queries run against this.
type CodeIndex = {
    Chunks: ChunkEntry[]
    CodeEmbeddings: float32[][]
    SummaryEmbeddings: float32[][]
    Imports: (string * string)[]       // (filePath, importedModule)
    TypeRefs: (string * string[])[]    // (filePath, typeNames)
    EmbeddingDim: int
}

/// Signature extracted from tree-sitter AST.
type DeclSignature = {
    Name: string
    Kind: string
    Signature: string
    FilePath: string
    StartLine: int
}

/// Import edge extracted from source.
type FileImport = {
    FilePath: string
    Module: string
    Line: int
    Raw: string
}

/// Type references extracted from source.
type FileTypeRef = {
    FilePath: string
    TypeRefs: string[]
}

/// File hash for incremental indexing.
type FileHash = {
    FilePath: string
    Hash: string
}
