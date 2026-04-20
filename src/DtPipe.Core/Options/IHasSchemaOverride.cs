namespace DtPipe.Core.Options;

/// <summary>
/// Implemented by reader options that support receiving a pre-compiled Arrow schema
/// (e.g., from an exported job YAML or --schema-load), bypassing type inference.
/// </summary>
public interface IHasSchemaOverride
{
    string Schema { get; set; }
}
