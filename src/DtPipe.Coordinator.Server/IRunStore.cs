namespace DtPipe.Coordinator.Server;

/// <summary>
/// A run as the store keeps it: its serialized document, whether it is over (it then enters the history instead of the runs in
/// flight) and <see cref="At"/>, the instant that orders the history.
/// </summary>
public sealed record StoredRun(string RunId, string Document, bool Over, DateTimeOffset At);

/// <summary>What <see cref="IRunStore.Load"/> found: the documents of runs not over, and those of the latest runs that are, newest first.</summary>
public sealed record StoredRuns(IReadOnlyList<string> InFlight, IReadOnlyList<string> History);

/// <summary>The store cannot be reached or did not answer in time: nothing it was asked to keep was kept.</summary>
public sealed class StoreUnavailableException(string message, Exception? inner = null) : Exception(message, inner);

/// <summary>
/// What <see cref="RunJournal"/> keeps beyond the process: the runs by id and the counter that gives their ids. One coordinator
/// writes it. A store holds documents and indexes, never the shape of a run: the journal serializes.
/// A call that cannot reach the store throws <see cref="StoreUnavailableException"/> having changed nothing.
/// </summary>
public interface IRunStore
{
    /// <summary>The runs that are over a store keeps in its history.</summary>
    const int Window = 200;

    /// <summary>The next id number, one greater than any it gave: it is atomic and never goes back.</summary>
    long NextSequence();

    /// <summary>
    /// Raises the counter to at least <paramref name="floor"/>, the highest id number among the runs read, and never lowers it. A
    /// store whose counter is absent and that holds no run (<paramref name="floor"/> is 0) was never started, or lost its data, and
    /// cannot tell which: it refuses unless it was told to initialize.
    /// </summary>
    void RaiseSequenceTo(long floor);

    /// <summary>
    /// Writes the document of the run; a run that is over leaves the runs in flight for the history, which is cut back to
    /// <see cref="Window"/>, and the documents it cuts go.
    /// </summary>
    void Put(StoredRun run);

    /// <summary>The runs in flight and the latest <see cref="Window"/> runs that are over, newest first.</summary>
    StoredRuns Load();
}
