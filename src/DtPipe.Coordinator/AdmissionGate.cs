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
    /// <exception cref="OperationCanceledException">
    /// <paramref name="ct"/> itself fired - a requester cancellation, never reported as a named
    /// refusal: the two are different callers and a caller that cancelled its own wait did not
    /// have any fragment refuse it.
    /// </exception>
    public async Task<IReadOnlyList<RegisteredNode>> AwaitAllAsync(
        IReadOnlyList<string> requiredFragments, TimeSpan timeout, CancellationToken ct = default)
    {
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

        while (true)
        {
            var found = requiredFragments
                .Select(name => _registry.TryGetByFragment(name))
                .ToList();

            if (found.All(n => n is not null))
                return found!;

            ct.ThrowIfCancellationRequested();

            if (timeoutCts.IsCancellationRequested)
            {
                var absent = requiredFragments
                    .Where((name, i) => found[i] is null)
                    .ToList();
                throw new AdmissionRefusedException(absent);
            }

            try { await Task.Delay(PollInterval, linked.Token); }
            catch (OperationCanceledException) { /* loop once more to tell which side fired above */ }
        }
    }
}
