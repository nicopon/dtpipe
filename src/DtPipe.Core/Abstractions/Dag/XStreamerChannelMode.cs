namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// Indicates which channel protocol the XStreamer expects its upstream branches to use.
/// The orchestrator reads this property at DAG setup time and configures the upstream
/// branch output accordingly — no CLI flag parsing needed.
/// </summary>
public enum XStreamerChannelMode
{
    /// <summary>
    /// Upstream branches write IReadOnlyList&lt;object?[]&gt; batches into a MemoryChannel.
    /// Used by NativeJoinXStreamer (row-oriented, .NET native types).
    /// </summary>
    Native,

    /// <summary>
    /// Upstream branches write Apache.Arrow RecordBatches into an ArrowMemoryChannel.
    /// Used by DuckXStreamer (columnar, zero-copy Arrow stream delivered to DuckDB via IArrowArrayStream).
    /// </summary>
    Arrow
}
