using Apache.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// An observation point on the execution path. The engine offers each stage's output to the
/// tap at the places it already reports progress, so a sample of a pipeline is a by-product
/// of the run that would have happened — not a second walk over the data with its own
/// semantics. A run without sampling has no tap and the offer is skipped.
///
/// Stage numbering: 0 is the reader's output, 1..n follow the transformers in pipeline order.
///
/// Two rules an implementation must honour:
/// <list type="bullet">
/// <item>It may only READ. Altering what flows would make the sampled run differ from the
/// real one, which is the whole thing this seam exists to prevent.</item>
/// <item>It must not dispose a <see cref="RecordBatch"/> it is shown, nor keep a reference to
/// one past the call — the segment runner still owns it (.claude/rules/engine-invariants.md › "RecordBatch
/// ownership"). Extract the values needed and let go.</item>
/// </list>
/// </summary>
public interface ISampleTap
{
    /// <summary>
    /// False once every stage has all the rows it wants, letting the engine skip the offer
    /// entirely for the rest of the run.
    /// </summary>
    bool WantsMore { get; }

    /// <summary>Declares a stage before any row is offered for it.</summary>
    void OnStageSchema(int stageIndex, string stageName, IReadOnlyList<PipeColumnInfo> schema, bool isColumnar);

    /// <summary>Offers one row leaving <paramref name="stageIndex"/>.</summary>
    void OnRow(int stageIndex, IReadOnlyList<object?> row);

    /// <summary>Offers one batch leaving <paramref name="stageIndex"/>. Read-only, not owned.</summary>
    void OnBatch(int stageIndex, RecordBatch batch);
}
