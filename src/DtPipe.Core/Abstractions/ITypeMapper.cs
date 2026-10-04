using DtPipe.Core.Models;

namespace DtPipe.Core.Abstractions;

/// <summary>How a dialect spells a fixed-point column and how wide it lets one be.</summary>
/// <param name="Keyword">The type name taking <c>(precision, scale)</c>.</param>
/// <param name="MaxPrecision">Widest precision the engine accepts.</param>
/// <param name="MaxScale">Largest scale the engine accepts, never above the precision.</param>
public sealed record DecimalSpelling(string Keyword, int MaxPrecision, int MaxScale)
{
    /// <summary>
    /// The declaration for a column that declares <paramref name="precision"/> and
    /// <paramref name="scale"/>, or null when it declares none or the engine cannot hold it.
    /// </summary>
    public string? Spell(int? precision, int? scale)
        => precision is int p && scale is int s
           && p >= 1 && p <= MaxPrecision && s >= 0 && s <= p && s <= MaxScale
            ? $"{Keyword}({p},{s})"
            : null;
}

/// <summary>
/// Interface for bidirectional type mapping between CLR types and database-specific types.
/// Each database adapter implements this with its own mapping logic.
/// </summary>
public interface ITypeMapper
{
    /// <summary>
    /// Maps a CLR type to the corresponding provider-specific type string.
    /// Used when generating CREATE TABLE statements from source column metadata.
    /// Example: typeof(DateTime) → "TIMESTAMP" (Oracle), "TIMESTAMP" (PG), "DATETIME2" (SQL Server)
    /// </summary>
    string MapToProviderType(Type clrType);

    /// <summary>
    /// How this dialect spells a declared decimal, or null when it has no fixed-point type to
    /// honour one with (SQLite stores a float whatever is declared).
    /// </summary>
    DecimalSpelling? DecimalType => null;

    /// <summary>
    /// Maps a source column. The CLR type fixes the native type except for a decimal the source
    /// declared a precision and scale for, which keeps them within the dialect's limits; anything
    /// undeclared or out of range takes the dialect's default for the CLR type. This is the single
    /// place that decision is made, for CREATE TABLE and for ADD COLUMN alike.
    /// </summary>
    string MapToProviderType(PipeColumnInfo column)
    {
        var underlying = Nullable.GetUnderlyingType(column.ClrType) ?? column.ClrType;
        return (underlying == typeof(decimal) ? DecimalType?.Spell(column.Precision, column.Scale) : null)
               ?? MapToProviderType(column.ClrType);
    }

    /// <summary>
    /// Maps a provider-specific type string to the corresponding CLR type.
    /// Used when introspecting target schema to determine what CLR type a column expects.
    /// Example: "TIMESTAMP" → typeof(DateTime), "VARCHAR2" → typeof(string)
    /// </summary>
    Type MapFromProviderType(string providerType);

    /// <summary>
    /// Builds the full native type string from schema parameters.
    /// Used when generating CREATE TABLE from introspected schema (BuildCreateTableFromIntrospection).
    /// Example: ("VARCHAR2", length=100) → "VARCHAR2(100)", ("NUMBER", precision=10, scale=2) → "NUMBER(10,2)"
    /// If the adapter doesn't need this level of detail, return dataType unchanged.
    /// </summary>
    string BuildNativeType(string dataType, int? dataLength, int? precision, int? scale, int? charLength);
}
