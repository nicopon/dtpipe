using System.Security.Claims;
using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>A host that presented a valid token the rights do not know, and what it proposes for itself. Nothing here is trusted.</summary>
public sealed record PendingHost(
    string Subject, string ProposedName, string Description, IReadOnlyList<string> Bricks, IReadOnlyDictionary<string, string> Labels,
    DateTimeOffset FirstSeen, DateTimeOffset LastSeen);

/// <summary>What <see cref="EnrollmentQueue.Propose"/> did with an announcement.</summary>
public enum Proposal
{
    /// <summary>A subject seen for the first time: it joins the queue.</summary>
    Recorded,
    /// <summary>A subject already waiting, whose proposal changed.</summary>
    Changed,
    /// <summary>A subject already waiting, whose proposal did not change.</summary>
    Unchanged,
    /// <summary>The subject was set aside by an administrator, and stays out until that expires.</summary>
    Ignored,
    /// <summary>The queue is full, and this subject is not in it. Reported once per subject.</summary>
    Full,
    /// <summary>The queue is full and this subject was already told so.</summary>
    FullAgain,
    /// <summary>The subject is no usable key.</summary>
    Invalid,
}

/// <summary>
/// The hosts waiting for an administrator: a valid token whose subject the rights do not know. One
/// entry per subject, in memory (a host announces itself again every few seconds, so a restart loses
/// nothing), bounded in number, and expired. What a host proposes is untrusted text: every string is
/// cut to a length here and shown escaped by the page, and none is ever an authority. A full queue
/// refuses a new subject and never evicts a waiting one. The hosts an administrator set aside are
/// kept in an <see cref="IIgnoredHostStore"/>: read when the queue is built and written through by
/// <see cref="Ignore"/>, so that an announcement, which asks whether its subject is set aside, never
/// waits on the store.
/// </summary>
public sealed class EnrollmentQueue
{
    public const int MaxPending = 50;
    public static readonly TimeSpan Lifetime = TimeSpan.FromHours(24);
    public const int MaxSubject = 128, MaxName = 63, MaxDescription = 200, MaxOrigin = 120, MaxBricks = 20, MaxBrick = 80, MaxLabels = 10, MaxLabelKey = 40, MaxLabelValue = 120;
    private const int MaxIgnored = 500;

    private readonly IIgnoredHostStore _store;
    private readonly TimeProvider _time;
    private readonly object _lock = new();
    private readonly Dictionary<string, PendingHost> _pending = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTimeOffset> _ignored = new(StringComparer.Ordinal);
    private readonly HashSet<string> _toldFull = new(StringComparer.Ordinal);

    /// <summary>Starts with the hosts the store says are still set aside; a store that cannot be read stops the start.</summary>
    public EnrollmentQueue(IIgnoredHostStore store, TimeProvider? time = null)
    {
        (_store, _time) = (store, time ?? TimeProvider.System);
        foreach (var (subject, until) in store.Load(_time.GetUtcNow())) _ignored[subject] = until;
    }

    public Proposal Propose(string? subject, NodeAnnouncement announcement, IReadOnlyDictionary<string, string> labels)
    {
        if (string.IsNullOrWhiteSpace(subject) || subject.Length > MaxSubject) return Proposal.Invalid;
        lock (_lock)
        {
            var now = _time.GetUtcNow();
            Expire(now);
            if (_ignored.ContainsKey(subject)) return Proposal.Ignored;
            var proposed = new PendingHost(
                subject, Clean(announcement.Name, MaxName), Clean(announcement.Description, MaxDescription),
                (announcement.Bricks ?? []).Take(MaxBricks).Select(b => Clean(b.Title, MaxBrick)).ToList(),
                labels.Take(MaxLabels).ToDictionary(kv => Clean(kv.Key, MaxLabelKey), kv => Clean(kv.Value, MaxLabelValue)),
                now, now);
            if (!_pending.TryGetValue(subject, out var known))
            {
                if (_pending.Count >= MaxPending) return _toldFull.Add(subject) ? Proposal.Full : Proposal.FullAgain;
                _pending[subject] = proposed;
                return Proposal.Recorded;
            }
            var changed = known.ProposedName != proposed.ProposedName || known.Description != proposed.Description
                || !known.Bricks.SequenceEqual(proposed.Bricks) || !known.Labels.OrderBy(kv => kv.Key).SequenceEqual(proposed.Labels.OrderBy(kv => kv.Key));
            _pending[subject] = proposed with { FirstSeen = known.FirstSeen };
            return changed ? Proposal.Changed : Proposal.Unchanged;
        }
    }

    public IReadOnlyList<PendingHost> List()
    {
        lock (_lock)
        {
            Expire(_time.GetUtcNow());
            return _pending.Values.OrderBy(p => p.FirstSeen).ThenBy(p => p.Subject, StringComparer.Ordinal).ToList();
        }
    }

    public PendingHost? Find(string subject)
    {
        lock (_lock)
        {
            Expire(_time.GetUtcNow());
            return _pending.GetValueOrDefault(subject);
        }
    }

    /// <summary>Takes a subject out of the queue: it was accepted, or added by hand.</summary>
    public bool Remove(string subject)
    {
        lock (_lock)
        {
            _toldFull.Remove(subject);
            return _pending.Remove(subject);
        }
    }

    /// <summary>
    /// Sets a subject aside for <see cref="Lifetime"/>: without this it would come back at the host's next attempt. The store keeps
    /// it first, outside the lock every announcement takes; one that refuses leaves the host waiting and throws.
    /// </summary>
    public void Ignore(string subject)
    {
        var now = _time.GetUtcNow();
        var until = now + Lifetime;
        _store.Add(subject, until, now, MaxIgnored);
        lock (_lock)
        {
            Expire(now);
            _pending.Remove(subject);
            if (_ignored.Count >= MaxIgnored && !_ignored.ContainsKey(subject))
                _ignored.Remove(_ignored.MinBy(kv => kv.Value).Key);
            _ignored[subject] = until;
        }
    }

    private void Expire(DateTimeOffset now)
    {
        foreach (var subject in _pending.Where(kv => now - kv.Value.LastSeen > Lifetime).Select(kv => kv.Key).ToList()) _pending.Remove(subject);
        foreach (var subject in _ignored.Where(kv => kv.Value <= now).Select(kv => kv.Key).ToList()) _ignored.Remove(subject);
        if (_pending.Count < MaxPending) _toldFull.Clear();
    }

    /// <summary>The text cut to <paramref name="max"/> characters, with control characters turned into spaces.</summary>
    internal static string Clean(string? text, int max)
    {
        var cleaned = new string((text ?? "").Select(c => char.IsControl(c) ? ' ' : c).ToArray()).Trim();
        return cleaned.Length > max ? cleaned[..max] : cleaned;
    }

    /// <summary>The label a token carries for each claim an administrator listed, the first value of each.</summary>
    public static IReadOnlyDictionary<string, string> LabelsOf(ClaimsPrincipal? user, IReadOnlyList<string> claimTypes)
    {
        var labels = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var type in claimTypes)
            if (user?.FindFirst(type)?.Value is { Length: > 0 } value) labels[type] = value;
        return labels;
    }
}
