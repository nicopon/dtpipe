using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Contracts;

/// <summary>
/// The schema a pipeline actually produces at its writer boundary, plus what identifies it.
///
/// <para>
/// A contract is <em>captured</em>, never declared: it is read off the real execution path —
/// transformers, DuckDB SQL and the row↔columnar bridges included — by the engine that will run
/// the job. That is the whole of its originality, and the first thing a careless change destroys.
/// </para>
///
/// <para>
/// <b>What it does not promise.</b> A contract fixes columns and types. It says nothing about the
/// invariants the target service enforces in its own code — a row can satisfy every type here and
/// still be rejected, or silently accepted, by rules that live in an ORM. "The contract is green"
/// must never be read as "the write is safe".
/// </para>
/// </summary>
public sealed record DataContract
{
    public const int CurrentVersion = 1;

    /// <summary>Format version of this file, so a later reader can refuse what it cannot read.</summary>
    public int Version { get; init; } = CurrentVersion;

    /// <summary>SHA-256 of the canonical schema JSON. This is what identifies a contract.</summary>
    public required string Hash { get; init; }

    /// <summary>
    /// <c>batch</c> when the schema came off a real <see cref="RecordBatch"/>, <c>columns</c> when
    /// the run produced none and it was derived from the column list instead. The difference is
    /// worth carrying: a column-derived schema is flat, so a struct or a list the reader really
    /// publishes is absent from it.
    /// </summary>
    public required string SchemaSource { get; init; }

    /// <summary>
    /// What the capturing run could guarantee about not writing to its <em>source</em>. Copied
    /// from the sample report rather than assumed: SQL Server has no read-only session, so the
    /// weaker guarantee has to travel with the contract instead of being rounded up.
    /// </summary>
    public string? Enforcement { get; init; }

    /// <summary>The dtpipe build that captured it.</summary>
    public string? Producer { get; init; }

    /// <summary>Which branch of the producing DAG this contract describes.</summary>
    public string? BranchAlias { get; init; }

    /// <summary>The Arrow schema itself, in <see cref="ArrowSchemaSerializer"/> form.</summary>
    public required JsonNode Schema { get; init; }

    /// <summary>
    /// Builds a contract from the schema a run produced.
    /// </summary>
    public static DataContract FromSchema(
        Schema schema,
        string schemaSource,
        string? enforcement = null,
        string? branchAlias = null)
        => new()
        {
            Hash = ComputeHash(schema),
            SchemaSource = schemaSource,
            Enforcement = enforcement,
            BranchAlias = branchAlias,
            Producer = ProducerVersion(),
            Schema = JsonNode.Parse(ArrowSchemaSerializer.SerializePretty(schema))!,
        };

    /// <summary>
    /// SHA-256 over the compact serialization, lowercase hex.
    /// </summary>
    /// <remarks>
    /// This relies on <see cref="ArrowSchemaSerializer"/> emitting metadata keys in a fixed order.
    /// Hashing a representation that is merely <em>usually</em> stable gives a contract that
    /// changes on its own, which is worse than having none: every consumer's check turns red for
    /// a pipeline nobody touched. If the serializer ever stops sorting, this hash is wrong, not
    /// the callers.
    /// </remarks>
    public static string ComputeHash(Schema schema)
    {
        var canonical = ArrowSchemaSerializer.SerializeCompact(schema);
        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
        return Convert.ToHexStringLower(digest);
    }

    private static string? ProducerVersion()
        => Assembly.GetEntryAssembly()?
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

    // ── File form ────────────────────────────────────────────────────────────

    /// <remarks>
    /// The relaxed encoder is deliberate. The default one escapes anything that could matter when
    /// JSON is pasted into HTML — so <c>+</c> in a version becomes <c>+</c> and an accented
    /// column name becomes a run of <c>\uXXXX</c>. This file is committed and read in a diff, not
    /// embedded in a page. It does not affect <see cref="ComputeHash"/>, which hashes the
    /// serializer's own output and not this one.
    /// </remarks>
    private static readonly JsonSerializerOptions _write = new()
    {
        WriteIndented = true,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    /// <summary>
    /// Writes the contract to <paramref name="path"/>, creating the directory if needed.
    /// </summary>
    /// <remarks>
    /// The key order is fixed and the schema is pretty-printed because this file is meant to be
    /// committed and reviewed: a diff has to show a schema change, not a reshuffle. That is also
    /// why the path is explicit and never under <c>.dtpipe/</c>, which is a session — limited
    /// lifetime, encrypted, purged on a TTL.
    /// </remarks>
    public void Write(string path)
    {
        var dir = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

        var node = new JsonObject
        {
            ["version"] = Version,
            ["hash"] = Hash,
            ["schemaSource"] = SchemaSource,
        };
        if (Enforcement is not null) node["enforcement"] = Enforcement;
        if (Producer is not null) node["producer"] = Producer;
        if (BranchAlias is not null) node["branchAlias"] = BranchAlias;
        node["schema"] = Schema.DeepClone();

        File.WriteAllText(path, node.ToJsonString(_write) + Environment.NewLine);
    }

    /// <summary>Reads a contract written by <see cref="Write"/>.</summary>
    public static DataContract Read(string path)
    {
        var root = JsonNode.Parse(File.ReadAllText(path)) as JsonObject
            ?? throw new InvalidOperationException($"Contract file '{path}' is not a JSON object.");

        var version = root["version"]?.GetValue<int>() ?? 0;
        if (version > CurrentVersion)
            throw new InvalidOperationException(
                $"Contract file '{path}' is version {version}; this build reads up to {CurrentVersion}.");

        return new DataContract
        {
            Version = version,
            Hash = root["hash"]?.GetValue<string>()
                ?? throw new InvalidOperationException($"Contract file '{path}' has no hash."),
            SchemaSource = root["schemaSource"]?.GetValue<string>() ?? "batch",
            Enforcement = root["enforcement"]?.GetValue<string>(),
            Producer = root["producer"]?.GetValue<string>(),
            BranchAlias = root["branchAlias"]?.GetValue<string>(),
            Schema = root["schema"]
                ?? throw new InvalidOperationException($"Contract file '{path}' has no schema."),
        };
    }

    /// <summary>The Arrow schema this contract describes.</summary>
    public Schema ToArrowSchema() => ArrowSchemaSerializer.Deserialize(Schema.ToJsonString());
}
