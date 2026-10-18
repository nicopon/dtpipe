namespace DtPipe.Coordinator.Server;

/// <summary>
/// What <see cref="EnrollmentQueue"/> keeps beyond the process of the hosts an administrator set aside: a subject and when it is
/// let back in. The queue reads it once and answers from memory, so the store is asked on a write, never on an announcement. A call
/// that cannot reach the store throws <see cref="StoreUnavailableException"/>; one that timed out may have been applied.
/// </summary>
public interface IIgnoredHostStore
{
    /// <summary>The subjects still set aside at <paramref name="now"/>, each with when it is let back in.</summary>
    IReadOnlyDictionary<string, DateTimeOffset> Load(DateTimeOffset now);

    /// <summary>
    /// Sets <paramref name="subject"/> aside until <paramref name="until"/>. The store forgets what expired by <paramref name="now"/>
    /// and keeps at most <paramref name="max"/> subjects, the soonest to be let back in going first.
    /// </summary>
    void Add(string subject, DateTimeOffset until, DateTimeOffset now, int max);
}
