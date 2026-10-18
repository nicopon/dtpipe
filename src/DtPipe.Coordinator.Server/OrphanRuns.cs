using DtPipe.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Tells the replicas of a node to abandon the fragments of the runs a restart interrupted (<see cref="RunJournal.Orphans"/>).
/// The authority is the journal, never what a replica announces: the runs it names are the ones this coordinator started and
/// did not see end. The journal does not say which replica carried a fragment, so every replica of the node is asked once, when it
/// announces with a data plane client (its run handlers are bound by then); one that knows no such run answers false. A fragment
/// is no longer asked once a replica has answered true, since only one holds it, and one whose message did not arrive is asked
/// again at that replica's next announcement. The list lives in this process: a second restart before the replicas announce forgets it.
/// </summary>
public sealed class OrphanRuns
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private readonly INodeLink _link;
    private readonly ILogger<OrphanRuns> _logger;
    private readonly object _lock = new();
    private readonly List<Owed> _owed;

    private sealed record Owed(string RunId, string Fragment, string Node)
    {
        public bool Done;
        public readonly HashSet<ReplicaKey> Asked = [];
    }

    public OrphanRuns(RunJournal journal, INodeLink link, ILogger<OrphanRuns> logger)
    {
        _link = link;
        _logger = logger;
        _owed = [.. journal.Orphans.SelectMany(run => run.Plan!.Fragments.Select(f => new Owed(run.RunId, f.Name, f.Node)))];
    }

    /// <summary>A replica of <paramref name="node"/> announced with a data plane client: asks it, off the hub's thread, to abandon what is owed there.</summary>
    public void OnAnnounced(ReplicaKey replica, string node)
    {
        List<Owed> asks;
        lock (_lock)
        {
            asks = _owed.Where(o => !o.Done && o.Node == node && o.Asked.Add(replica)).ToList();
        }
        // A replica's client takes messages one after another and the announcement is still being answered: the asks go out after it.
        foreach (var owed in asks) _ = Task.Run(() => AskAsync(replica, owed));
    }

    private async Task AskAsync(ReplicaKey replica, Owed owed)
    {
        try
        {
            using var bound = new CancellationTokenSource(Bound);
            if (await _link.CancelAsync(replica, owed.RunId, owed.Fragment, bound.Token))
            {
                lock (_lock) owed.Done = true;
                _logger.LogWarning("{Fragment} of run {RunId}, left by a restart, was abandoned on {Replica}", owed.Fragment, owed.RunId, replica);
            }
        }
        catch (Exception ex)
        {
            lock (_lock) owed.Asked.Remove(replica);
            _logger.LogWarning("Could not ask {Replica} to abandon {Fragment} of run {RunId}; it is asked again at its next announcement: {Error}",
                replica, owed.Fragment, owed.RunId, ex.Message);
        }
    }
}
