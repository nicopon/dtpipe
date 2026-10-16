using System.Text.Json;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>Why a pipeline's bindings do not hold, as <see cref="BindingProblem.Cause"/>.</summary>
public static class BindingCauses
{
    /// <summary>A branch is a brick and no binding names a node for it.</summary>
    public const string Unassigned = "unassigned";

    /// <summary>A binding names a node the coordinator knows no identity for.</summary>
    public const string UnknownNode = "unknown-node";

    /// <summary>A binding names a node that has no connected replica.</summary>
    public const string Offline = "offline";

    /// <summary>A binding names a node that does not offer the brick the branch is.</summary>
    public const string NotOffered = "not-offered";

    /// <summary>A binding names a branch that is no brick: only a brick is bound, and a bound branch never runs on the runner.</summary>
    public const string NotABrick = "not-a-brick";

    /// <summary>A binding names a branch the job does not have.</summary>
    public const string NoBranch = "no-branch";

    /// <summary>The brick a binding names is not the one the branch's text designates on the bound node.</summary>
    public const string BrickMismatch = "brick-mismatch";

    /// <summary>A binding asks for a mode other than <see cref="BindingModes.Strict"/>.</summary>
    public const string UnsupportedMode = "unsupported-mode";
}

/// <summary>
/// One reason a pipeline's bindings do not hold for a job. <see cref="Blocking"/>: the job cannot be deployed
/// as it stands; a node that is only offline does not block, since its replicas come back.
/// </summary>
public sealed record BindingProblem(string Alias, string Cause, string Message, bool Blocking);

/// <summary>
/// Where a pipeline's bricks run: the node and the brick each source or sink branch of its job is bound to, by the branch's
/// alias. The runner is never bound: it is stateless, and every branch that is no brick goes to it. The library keeps the
/// bindings beside the job, in <c>bindings.json</c>, so the job stays a plain dtpipe job.
/// </summary>
public static class Bindings
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>The mode of a binding: <see cref="BindingModes.Strict"/> when it names none.</summary>
    public static string ModeOf(Binding binding) => string.IsNullOrWhiteSpace(binding.Mode) ? BindingModes.Strict : binding.Mode;

    public static string Serialize(IReadOnlyDictionary<string, Binding> bindings) =>
        JsonSerializer.Serialize(
            new SortedDictionary<string, Binding>(
                bindings.ToDictionary(b => b.Key, b => b.Value with { Mode = ModeOf(b.Value) }), StringComparer.Ordinal), Json);

    /// <summary>The hash a saved plan keeps of the bindings it was made from; null for none, so no bindings and no file are the same.</summary>
    public static string? Sha(IReadOnlyDictionary<string, Binding> bindings) =>
        bindings.Count == 0 ? null : PipelineLibrary.Sha(Serialize(bindings));
}
