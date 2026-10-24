namespace DtPipe.Core.Abstractions.Dag;

public enum DagLogSeverity
{
    Warning,
    Error
}

/// <summary>
/// What the orchestrator reports while a DAG runs.
/// </summary>
/// <param name="Severity">Whether the DAG stopped on a fault or was cancelled.</param>
/// <param name="Message">Plain text: the host that shows it adds the glyphs and the markup, and escapes it.</param>
/// <param name="Branch">The branch the event is about, or null when it concerns the whole DAG.</param>
public sealed record DagLogEvent(DagLogSeverity Severity, string Message, string? Branch = null);
