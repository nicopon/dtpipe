using Apache.Arrow.Types;

namespace DtPipe.Processors.DuckDB;

/// <summary>
/// The DuckDB SQL type whose result the <c>--sql</c> processor hands on as a given Arrow type, so
/// a <c>CAST(col AS …)</c> written in a step arrives at the writer under that type. Null for an
/// Arrow type no DuckDB cast produces.
/// </summary>
public static class DuckDbSqlTypeNames
{
    /// <summary>
    /// The DuckDB expression that turns <paramref name="column"/>, received as <paramref name="received"/>,
    /// into <paramref name="accepted"/>; null when no cast produces that type. An instant becomes the
    /// UTC wall clock: a plain cast would read it in the session's zone, which is the machine's.
    /// </summary>
    public static string? CastTo(string column, IArrowType received, IArrowType accepted)
    {
        if (ForArrow(accepted) is not { } target) return null;
        if (received is TimestampType { Timezone: { Length: > 0 } } && accepted is TimestampType { Timezone: null or "" })
            return $"timezone('UTC', {column})";
        return $"CAST({column} AS {target})";
    }

    public static string? ForArrow(IArrowType type) => type switch
    {
        // Decimal types derive from FixedSizeBinaryType: test them first.
        Decimal128Type d => $"DECIMAL({d.Precision},{d.Scale})",
        Decimal256Type d => $"DECIMAL({d.Precision},{d.Scale})",
        BooleanType => "BOOLEAN",
        Int8Type => "TINYINT",
        Int16Type => "SMALLINT",
        Int32Type => "INTEGER",
        Int64Type => "BIGINT",
        UInt8Type => "UTINYINT",
        UInt16Type => "USMALLINT",
        UInt32Type => "UINTEGER",
        UInt64Type => "UBIGINT",
        FloatType => "FLOAT",
        DoubleType => "DOUBLE",
        StringType => "VARCHAR",
        BinaryType => "BLOB",
        Date32Type => "DATE",
        Time64Type => "TIME",
        TimestampType ts => string.IsNullOrEmpty(ts.Timezone) ? "TIMESTAMP" : "TIMESTAMPTZ",
        _ => null
    };
}
