namespace DtPipe.Core.Options;

/// <summary>
/// Provides CLI flag names for IOptionSet properties, keyed by property name.
/// Allows IOptionSet implementations outside DtPipe to declare CLI bindings
/// without depending on System.CommandLine or CLI-specific attributes.
/// </summary>
public interface ICliOptionMetadata
{
    /// <summary>
    /// Maps property name → CLI flag (e.g., "MainAlias" → "--main")
    /// </summary>
    IReadOnlyDictionary<string, string> PropertyToFlag { get; }
}
