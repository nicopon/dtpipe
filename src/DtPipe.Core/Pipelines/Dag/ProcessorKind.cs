namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Defines the type of processor applied to a branch confluence.
/// </summary>
public enum ProcessorKind
{
    /// <summary>
    /// No processor applied (standard linear or fan-out branch).
    /// </summary>
    None,

    /// <summary>
    /// DataFusion SQL engine processor.
    /// </summary>
    Sql
}
