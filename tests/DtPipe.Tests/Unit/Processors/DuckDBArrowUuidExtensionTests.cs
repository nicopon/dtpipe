using Apache.Arrow;
using DtPipe.Adapters.Shared.Infrastructure.DuckDb;
using DuckDB.NET.Data;
using Xunit;

namespace DtPipe.Tests.Unit.Processors;

/// <summary>
/// Documents and validates DuckDB's Arrow C Data Interface behavior for UUID columns.
///
/// Key findings:
/// - By DEFAULT, DuckDB exports UUID as Utf8 (StringType) — no extension metadata.
/// - With SET arrow_lossless_conversion = true, DuckDB exports UUID as
///   FixedSizeBinary(16) + ARROW:extension:name=arrow.uuid (canonical Arrow spec).
///
/// DuckDBSqlProcessor sets arrow_lossless_conversion = true in OpenAsync, so the
/// Arrow-native schema probe returns the correct type.
/// </summary>
public class DuckDBArrowUuidExtensionTests
{
    private readonly ITestOutputHelper _output;
    public DuckDBArrowUuidExtensionTests(ITestOutputHelper output) => _output = output;

    private const string KnownUuid = "SELECT '550e8400-e29b-41d4-a716-446655440000'::UUID AS id";

    /// <summary>
    /// Documents the DEFAULT DuckDB behavior: UUID exports as Utf8, no extension metadata.
    /// This is the raw behavior without any configuration — DuckDBSqlProcessor overrides it.
    /// </summary>
    [Fact]
    public async Task DuckDB_Default_UuidColumn_ExportsAsUtf8_NoExtensionMetadata()
    {
        await using var conn = new DuckDBConnection("DataSource=:memory:");
        await conn.OpenAsync();
        // No SET arrow_lossless_conversion — raw default behavior.

        var schema = QuerySchema(conn, KnownUuid);
        var idField = Assert.Single(schema.FieldsList);

        _output.WriteLine($"Arrow type    : {idField.DataType.GetType().Name}");
        _output.WriteLine($"Field metadata: {FormatMeta(idField.Metadata)}");

        Assert.IsType<Apache.Arrow.Types.StringType>(idField.DataType);
        Assert.True(idField.Metadata is null || !idField.Metadata.ContainsKey("ARROW:extension:name"));
    }

    /// <summary>
    /// Validates that SET arrow_lossless_conversion = true makes DuckDB emit
    /// FixedSizeBinary(16) + arrow.uuid — the canonical Arrow UUID extension.
    /// This is the behavior DuckDBSqlProcessor relies on.
    /// </summary>
    [Fact]
    public async Task DuckDB_LosslessConversion_UuidColumn_ExportsAsFixedSizeBinaryWithArrowUuidExtension()
    {
        await using var conn = new DuckDBConnection("DataSource=:memory:");
        await conn.OpenAsync();

        using (var set = conn.CreateCommand())
        {
            set.CommandText = "SET arrow_lossless_conversion = true";
            await set.ExecuteNonQueryAsync();
        }

        var schema = QuerySchema(conn, KnownUuid);
        var idField = Assert.Single(schema.FieldsList);

        _output.WriteLine($"Arrow type    : {idField.DataType.GetType().Name}");
        _output.WriteLine($"Field metadata: {FormatMeta(idField.Metadata)}");

        Assert.IsType<Apache.Arrow.Types.FixedSizeBinaryType>(idField.DataType);
        Assert.Equal(16, ((Apache.Arrow.Types.FixedSizeBinaryType)idField.DataType).ByteWidth);

        string? extensionName = null;
        idField.Metadata?.TryGetValue("ARROW:extension:name", out extensionName);
        Assert.Equal("arrow.uuid", extensionName, StringComparer.OrdinalIgnoreCase);
    }

    private static Schema QuerySchema(DuckDBConnection conn, string query)
    {
        using var reader = new DuckDbArrowResultReader(conn, query);
        return reader.Prepare();
    }

    private static string FormatMeta(IReadOnlyDictionary<string, string>? meta) =>
        meta is null || meta.Count == 0 ? "(none)"
        : string.Join(", ", meta.Select(kv => $"{kv.Key}={kv.Value}"));
}
