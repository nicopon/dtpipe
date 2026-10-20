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

    /// <summary>A referenced brick is no longer at the version a check pinned.</summary>
    public const string BrickChanged = "brick-changed";

    /// <summary>A binding names a branch the job does not have.</summary>
    public const string NoBranch = "no-branch";

    /// <summary>The brick a binding names is not the one the branch's text designates on the bound node.</summary>
    public const string BrickMismatch = "brick-mismatch";

    /// <summary>A binding asks for a mode that is none of <see cref="BindingModes.All"/>.</summary>
    public const string UnsupportedMode = "unsupported-mode";

    /// <summary>A binding in a mode other than <see cref="BindingModes.Strict"/> has no pin yet, or none that holds what its mode compares: a check writes it.</summary>
    public const string Unchecked = "unchecked";
}

/// <summary>What a designer's check pinned for an alias: the brick it sampled (by id on the bound node) and what it saw of it.</summary>
public sealed record CheckedPin(string Brick, BindingPin Pin);

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

    public static bool IsStrict(Binding binding) => ModeOf(binding) == BindingModes.Strict;

    /// <summary>
    /// <paramref name="requested"/> set into <paramref name="held"/>, entry by entry: an alias the request does not name is kept, and
    /// one it names without a mode keeps the mode the library holds for it (a publisher that knows no mode must not turn a
    /// <c>tolerant</c> binding back into <c>strict</c>). A request never brings a pin: only a check writes one.
    /// </summary>
    public static Dictionary<string, Binding> Merge(IReadOnlyDictionary<string, Binding> held, IReadOnlyDictionary<string, Binding>? requested)
    {
        var merged = new Dictionary<string, Binding>(held, StringComparer.Ordinal);
        foreach (var (alias, binding) in requested ?? new Dictionary<string, Binding>())
            merged[alias] = binding with { Mode = binding.Mode ?? held.GetValueOrDefault(alias)?.Mode, Pin = null };
        return merged;
    }

    /// <summary>
    /// <paramref name="bindings"/> as a check leaves them: a strict one has no pin (its text is the pin), and one in another mode
    /// has the pin of the brick the check sampled, which it then names. A pin that does not hold what its mode compares (a contract
    /// the node did not send) is left out, so the binding stays <see cref="BindingCauses.Unchecked"/> rather than pass for ever.
    /// </summary>
    public static Dictionary<string, Binding> Pinned(
        IReadOnlyDictionary<string, Binding> bindings, IReadOnlyDictionary<string, CheckedPin>? pins, IReadOnlySet<string>? referenced = null)
    {
        var pinned = new Dictionary<string, Binding>(StringComparer.Ordinal);
        foreach (var (alias, binding) in bindings)
        {
            var mode = ModeOf(binding);
            // A referenced brick has no text to be its own pin: every mode, strict included, pins the version it was checked at.
            if (mode == BindingModes.Strict && referenced?.Contains(alias) == true && pins?.GetValueOrDefault(alias) is { } version)
                pinned[alias] = binding with { Pin = new BindingPin(version.Pin.Version) };
            else if (mode == BindingModes.Strict) pinned[alias] = binding with { Pin = null };
            else if (pins?.GetValueOrDefault(alias) is { } seen && (mode == BindingModes.Tolerant || seen.Pin.Contract is not null))
                pinned[alias] = binding with { Brick = seen.Brick, Pin = mode == BindingModes.Tolerant ? seen.Pin with { Contract = null } : seen.Pin };
            else pinned[alias] = binding with { Pin = null };
        }
        return pinned;
    }

    /// <summary>The hash of one binding as it is kept, which a verdict cached for it is keyed by.</summary>
    public static string ShaOf(Binding binding) => PipelineLibrary.Sha(JsonSerializer.Serialize(binding with { Mode = ModeOf(binding) }, Json));

    public static string Serialize(IReadOnlyDictionary<string, Binding> bindings) =>
        JsonSerializer.Serialize(
            new SortedDictionary<string, Binding>(
                bindings.ToDictionary(b => b.Key, b => b.Value with { Mode = ModeOf(b.Value) }), StringComparer.Ordinal), Json);

    /// <summary>The hash an armed deployment keeps of the bindings it was armed with; null for none, so no bindings and no file are the same.</summary>
    public static string? Sha(IReadOnlyDictionary<string, Binding> bindings) =>
        bindings.Count == 0 ? null : PipelineLibrary.Sha(Serialize(bindings));
}
