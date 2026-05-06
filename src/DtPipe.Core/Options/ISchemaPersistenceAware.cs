namespace DtPipe.Core.Options;

/// <summary>
/// Implemented by text reader options that support schema caching (.dtschema files).
/// </summary>
public interface ISchemaPersistenceAware
{
    string? SchemaSave { get; set; }
    string? SchemaLoad { get; set; }
}
