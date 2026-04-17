using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Configuration;

/// <summary>
/// Persists and loads named schema definitions (.dtschema files).
/// The full Arrow <see cref="Schema"/> is stored — including nested StructType, ListType,
/// field metadata (arrow.uuid, …) — so all structural information survives a round-trip.
/// </summary>
public static class SchemaStore
{
    private const string DefaultDir = ".dtpipe/schemas";
    private const int CurrentVersion = 2;

    private static readonly JsonSerializerOptions _serializerOptions = new JsonSerializerOptions
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Saves the complete Arrow schema to a named .dtschema file.
    /// </summary>
    /// <param name="name">Schema name (used as filename, without extension).</param>
    /// <param name="readerType">Provider name (e.g. "jsonl", "xml", "csv").</param>
    /// <param name="schema">The full Arrow schema to persist.</param>
    /// <param name="schemaDir">Override directory. Defaults to .dtpipe/schemas in the current directory.</param>
    public static void Save(string name, string readerType, Schema schema, string? schemaDir = null)
    {
        var dir = schemaDir ?? DefaultDir;
        Directory.CreateDirectory(dir);

        var schemaJson = ArrowSchemaSerializer.SerializePretty(schema);
        // Embed as a nested JSON object (not a string) for readability.
        // We write the envelope manually so the schema is pretty-printed inline.
        var envelope = new SchemaEntry
        {
            Version    = CurrentVersion,
            BuiltAt    = DateTime.UtcNow.ToString("O"),
            ReaderType = readerType
        };
        var envelopeJson = JsonSerializer.Serialize(envelope, _serializerOptions);

        // Inject the schema object inline (replace the closing brace with schema field).
        // This produces a clean, indented file without double-encoding the schema as a string.
        var schemaIndented = IndentJson(schemaJson, "  ");
        var finalJson = envelopeJson.TrimEnd().TrimEnd('}')
            + $",\n  \"schema\": {schemaIndented}\n}}";

        File.WriteAllText(GetFilePath(name, dir), finalJson);
    }

    /// <summary>
    /// Loads the Arrow schema from a named .dtschema file.
    /// Returns null if the file does not exist.
    /// </summary>
    /// <exception cref="InvalidOperationException">If the file is corrupt or unreadable.</exception>
    public static Schema? Load(string name, string? schemaDir = null)
    {
        var filePath = GetFilePath(name, schemaDir ?? DefaultDir);
        if (!File.Exists(filePath)) return null;

        try
        {
            var json = File.ReadAllText(filePath);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (!root.TryGetProperty("schema", out var schemaEl))
                throw new InvalidOperationException("Missing 'schema' field.");

            return ArrowSchemaSerializer.Deserialize(schemaEl.GetRawText());
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Schema file '{filePath}' could not be read: {ex.Message}", ex);
        }
    }

    private static string GetFilePath(string name, string dir)
        => Path.Combine(dir, $"{name}.dtschema");

    /// <summary>Indents each line of a JSON string by <paramref name="indent"/>.</summary>
    private static string IndentJson(string json, string indent)
    {
        var lines = json.Split('\n');
        return string.Join('\n', lines.Select(l => indent + l));
    }

    // Used only for serializing the envelope without the schema field.
    private sealed class SchemaEntry
    {
        public int    Version    { get; set; }
        public string? BuiltAt   { get; set; }
        public string? ReaderType { get; set; }
    }
}
