namespace DtPipe.Coordinator;

/// <summary>Admission refused a run: at least one required fragment never registered in time.</summary>
public sealed class AdmissionRefusedException : Exception
{
    public IReadOnlyList<string> AbsentFragments { get; }

    public AdmissionRefusedException(IReadOnlyList<string> absentFragments)
        : base("Run refused, fragment(s) never registered: " + string.Join(", ", absentFragments))
    {
        AbsentFragments = absentFragments;
    }
}

/// <summary>
/// The rendez-vous barrier: a run starts only once every fragment it names has registered. No lot
/// launches a single child ahead of the others - an absent peer is a named, hard refusal, not a
/// stuck lone consumer.
/// </summary>
public sealed class AdmissionGate
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(20);

    private readonly INodeRegistry _registry;

    public AdmissionGate(INodeRegistry registry)
    {
        _registry = registry;
    }

    /// <exception cref="AdmissionRefusedException">
    /// At least one fragment in <paramref name="requiredFragments"/> was still unregistered when
    /// <paramref name="timeout"/> elapsed. Names every one still missing at that instant.
    /// </exception>
    public async Task<IReadOnlyList<RegisteredNode>> AwaitAllAsync(
        IReadOnlyList<string> requiredFragments, TimeSpan timeout, CancellationToken ct = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);

        while (true)
        {
            var found = requiredFragments
                .Select(name => _registry.TryGetByFragment(name))
                .ToList();

            if (found.All(n => n is not null))
                return found!;

            if (cts.IsCancellationRequested)
            {
                var absent = requiredFragments
                    .Where((name, i) => found[i] is null)
                    .ToList();
                throw new AdmissionRefusedException(absent);
            }

            try { await Task.Delay(PollInterval, cts.Token); }
            catch (OperationCanceledException) { /* loop once more to build the named refusal above */ }
        }
    }
}
