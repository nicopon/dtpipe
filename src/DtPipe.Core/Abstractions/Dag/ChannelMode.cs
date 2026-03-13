namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// Indicates which channel protocol a branch confluence processor expects its upstream branches to use.
/// The orchestrator reads this property at DAG setup time and configures the upstream
/// branch output accordingly.
/// </summary>
public enum ChannelMode
{
    /// <summary>
    /// Upstream branches write IReadOnlyList&lt;object?[]&gt; batches into a MemoryChannel.
    /// Used by row-oriented .NET native transformers.
    /// </summary>
    Native,

    /// <summary>
    /// Upstream branches write Apache.Arrow RecordBatches into an ArrowMemoryChannel.
    /// Used by columnar engines like DataFusion (zero-copy Arrow stream).
    /// </summary>
    Arrow
}
