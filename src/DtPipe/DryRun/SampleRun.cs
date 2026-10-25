using DtPipe.Core.Models;

namespace DtPipe.DryRun;

/// <summary>
/// The rows observed at one pipeline stage, with the schema in force there.
/// </summary>
/// <param name="TotalSeen">
/// How many rows passed the stage, capture quota or not. This is what lets a reader see that
/// a stage changed cardinality — a 1:N expansion or an N:1 window — which a "one row and its
/// stages" model cannot express: a windowed pipeline would read as dropping everything.
/// </param>
public sealed record StageCapture(
    int Index,
    string Name,
    IReadOnlyList<PipeColumnInfo> Schema,
    bool IsColumnar,
    IReadOnlyList<object?[]> Rows,
    long TotalSeen);

/// <summary>
/// What one sample-mode execution observed. Produced by the real engine through
/// <see cref="DtPipe.Core.Abstractions.ISampleTap"/>, and read by both the renderer and the
/// checkpoint store — one capture, one execution, so the two cannot disagree.
/// </summary>
public sealed record SampleRun(
    IReadOnlyList<StageCapture> Stages,
    long RowsRead,
    long RowsWritten);
