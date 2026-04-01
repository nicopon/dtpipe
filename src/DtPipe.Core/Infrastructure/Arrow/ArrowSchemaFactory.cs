using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Models;

namespace DtPipe.Core.Infrastructure.Arrow;

public static class ArrowSchemaFactory
{
    // Arrow canonical extension metadata for UUID columns (RFC 4122, FixedSizeBinary(16))
    private static readonly IReadOnlyDictionary<string, string> UuidMetadata =
        new Dictionary<string, string> { ["ARROW:extension:name"] = "arrow.uuid" };

    public static Schema Create(IReadOnlyList<PipeColumnInfo> columns)
    {
        var builder = new Schema.Builder();
        foreach (var col in columns)
        {
            var arrowType = ArrowTypeMapper.GetArrowType(col.ClrType);
            // Emit arrow.uuid extension metadata on Guid columns (including Nullable<Guid>) so the
            // schema is self-describing. Allows Arrow-aware consumers (PyArrow, DuckDB, etc.) to
            // recognise the UUID type without relying on storage-type heuristics.
            var clrBase = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;
            var metadata = clrBase == typeof(Guid) ? UuidMetadata : null;
            builder.Field(new Field(col.Name, arrowType, col.IsNullable, metadata));
        }
        return builder.Build();
    }
}
