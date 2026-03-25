namespace DtPipe.Core.Pipelines;

/// <summary>Describes how one step (reader, transformer) will execute: columnar or row.</summary>
public record PipelineExecutionStep(
    string Name,
    bool IsColumnarCapable,
    bool WillRunColumnar
);

/// <summary>
/// Describes how the pipeline will execute at runtime: columnar vs row paths, bridge count.
/// Computed during dry-run from reader/transformer/writer capabilities and row-sink preference.
/// </summary>
public record PipelineExecutionPlan(
    string ReaderName,
    bool ReaderIsColumnar,
    string WriterName,
    bool WriterIsColumnar,
    bool RowModePreferred,
    IReadOnlyList<PipelineExecutionStep> Steps,
    int BridgeCount
);
