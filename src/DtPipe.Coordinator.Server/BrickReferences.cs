using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The one pass that reads the brick references of a job (<c>input: brick:&lt;node&gt;/&lt;brick&gt;</c>) before anything else
/// meets its bindings. A reference names its node and its brick, so a binding of a referenced alias holds only the mode and the pin
/// (the version checked, and for the modes that compare it, the contract); <see cref="Expand"/> puts the node and the brick back
/// for everything that reads <see cref="Binding.Node"/> (admission, the queue, health), and <see cref="Strip"/> takes them out
/// again before <c>bindings.json</c> is written. A job without a reference passes through both unchanged.
/// </summary>
public static class BrickReferences
{
    /// <summary>The references of <paramref name="yaml"/>, by alias; none for a job that is not readable.</summary>
    public static IReadOnlyDictionary<string, BrickReference> In(string yaml)
    {
        try { return BrickRef.In(JobYaml.Parse(yaml)); }
        catch (Exception) { return new Dictionary<string, BrickReference>(); }
    }

    /// <summary><paramref name="bindings"/> with the node and the brick of every referenced alias taken from the reference, and an alias that has no binding bound in strict mode.</summary>
    public static IReadOnlyDictionary<string, Binding> Expand(string yaml, IReadOnlyDictionary<string, Binding> bindings)
    {
        var refs = In(yaml);
        if (refs.Count == 0) return bindings;
        var expanded = new Dictionary<string, Binding>(bindings, StringComparer.Ordinal);
        foreach (var (alias, reference) in refs)
        {
            var held = bindings.GetValueOrDefault(alias);
            expanded[alias] = new Binding(reference.Node, reference.Brick, held?.Mode, held?.Pin);
        }
        return expanded;
    }

    /// <summary><paramref name="bindings"/> as they are kept: the entry of a referenced alias holds its mode and its pin, never the node or the brick the job says.</summary>
    public static IReadOnlyDictionary<string, Binding> Strip(string yaml, IReadOnlyDictionary<string, Binding> bindings)
    {
        var refs = In(yaml);
        if (refs.Count == 0) return bindings;
        var stripped = new Dictionary<string, Binding>(bindings, StringComparer.Ordinal);
        foreach (var alias in refs.Keys)
            if (bindings.TryGetValue(alias, out var held) && (held.Mode is not null || held.Pin is not null))
                stripped[alias] = new Binding(null!, null, held.Mode, held.Pin);
            else stripped.Remove(alias);
        return stripped;
    }
}
