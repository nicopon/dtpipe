namespace DtPipe.Core.Abstractions;

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
