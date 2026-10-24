namespace DtPipe.Core.Abstractions;

/// <summary>
/// What a message given to <see cref="IExportObserver.LogMessage"/> is to the person reading it.
/// The message is plain text; the observer turns the tone into its own formatting.
/// </summary>
public enum MessageTone
{
    /// <summary>A step of the run, shown as it is.</summary>
    Plain,

    /// <summary>A secondary detail: what the run read or saved beside the rows.</summary>
    Detail,

    /// <summary>Something the person should notice before it surprises them.</summary>
    Notice,

    /// <summary>A step that recovered and let the run continue.</summary>
    Success
}
