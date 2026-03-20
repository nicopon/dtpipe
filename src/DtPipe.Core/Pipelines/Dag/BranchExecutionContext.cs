using System.Collections.Generic;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Scoped context for a single branch execution within a DAG.
/// Maps logical aliases (from SQL/CLI) to physical channel aliases (fan-out clones).
/// </summary>
public sealed class BranchExecutionContext
{
    /// <summary>
    /// Key: Logical Alias (e.g. "c") -> Value: Physical Channel Alias (e.g. "c__fan_0")
    /// </summary>
    public IReadOnlyDictionary<string, string> AliasMap { get; set; } = new Dictionary<string, string>();
}
