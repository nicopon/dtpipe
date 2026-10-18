using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// <see cref="IRunStore"/> in memory, for tests that are not about Redis: the same contract (documents, a counter that never
/// goes back, a history cut to the window), nothing from a file. A store lives as long as the test process, found again by the
/// name of the state directory a test gives it: a second <see cref="RunJournal"/> over the same name is a restart.
/// </summary>
public sealed class MemoryRunStore : IRunStore
{
    private static readonly ConcurrentDictionary<string, MemoryRunStore> Stores = new();

    private readonly object _lock = new();
    private readonly Dictionary<string, string> _documents = [];
    private readonly HashSet<string> _active = [];
    private readonly List<(string Id, DateTimeOffset At)> _history = [];
    private long _seq;

    /// <summary>The store a test names; the same one every time it is asked for the same name.</summary>
    public static MemoryRunStore At(string name) => Stores.GetOrAdd(name, _ => new MemoryRunStore());

    /// <summary>A journal over the store of <paramref name="options"/>' state directory.</summary>
    public static RunJournal Journal(CoordinatorServerOptions options) => new(At(options.StateDir), NullLogger<RunJournal>.Instance);

    /// <summary>The document kept for a run, or null.</summary>
    public string? Document(string runId)
    {
        lock (_lock) return _documents.GetValueOrDefault(runId);
    }

    /// <summary>Puts a document the journal did not write, as one left in the store by something else.</summary>
    public void Seed(string runId, string document, bool over)
    {
        lock (_lock)
        {
            _documents[runId] = document;
            if (over) _history.Add((runId, DateTimeOffset.UtcNow)); else _active.Add(runId);
        }
    }

    public long NextSequence()
    {
        lock (_lock) return ++_seq;
    }

    public void RaiseSequenceTo(long floor)
    {
        lock (_lock) _seq = Math.Max(_seq, floor);
    }

    public void Put(StoredRun run)
    {
        lock (_lock)
        {
            _documents[run.RunId] = run.Document;
            _history.RemoveAll(h => h.Id == run.RunId);
            if (run.Over)
            {
                _active.Remove(run.RunId);
                _history.Add((run.RunId, run.At));
                _history.Sort((a, b) => b.At.CompareTo(a.At));
                while (_history.Count > IRunStore.Window)
                {
                    _documents.Remove(_history[^1].Id);
                    _history.RemoveAt(_history.Count - 1);
                }
            }
            else _active.Add(run.RunId);
        }
    }

    public StoredRuns Load()
    {
        lock (_lock)
            return new StoredRuns([.. _active.Select(id => _documents[id])], [.. _history.Take(IRunStore.Window).Select(h => _documents[h.Id])]);
    }
}
