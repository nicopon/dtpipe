namespace DtPipe.Core.Options;

/// <summary>
/// Implemented by writer/reader options that carry a target table concept: a typed capability,
/// so no caller looks the table up by reflection (<c>GetProperty("Table")</c>).
/// </summary>
public interface ITableAwareOptions
{
    string? Table { get; }
}

