namespace QueryDump.Core.Abstractions;

/// <summary>
/// Base factory interface for data components (Readers/Writers) that can be selected via connection string.
/// </summary>
public interface IDataFactory
{
    /// <summary>
    /// Unique provider name (e.g., "duck", "ora", "csv").
    /// Used for deterministic prefix-based resolution (e.g. "duck:my.db") and display.
    /// </summary>
    string ProviderName { get; }

    /// <summary>
    /// Determines if this factory can handle the given connection string or file path.
    /// Used as a fallback when no prefix is provided.
    /// </summary>
    bool CanHandle(string connectionString);
}
