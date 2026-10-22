namespace DtPipe.Coordinator.Server;

/// <summary>A cursor as the store keeps it: the pipeline and the sink alias that key it, and its serialized document.</summary>
public sealed record StoredCursor(string Pipeline, string Alias, string Document);

/// <summary>
/// What <see cref="CursorKeeper"/> keeps beyond the process of the cursors of the library's pipelines: one serialized document per
/// (pipeline, sink alias). One coordinator writes it. A store holds the text, never the shape of a cursor. A call that cannot reach the
/// store throws <see cref="StoreUnavailableException"/> having changed nothing the caller can rely on: a call that timed out may have
/// been applied.
/// </summary>
public interface ICursorStore
{
    /// <summary>Every document the store holds, in no order.</summary>
    IReadOnlyList<StoredCursor> Load();

    /// <summary>Keeps <paramref name="document"/> as the cursor's, replacing the one it held.</summary>
    void Save(string pipeline, string alias, string document);

    /// <summary>Forgets the cursor's document; nothing happens when it has none.</summary>
    void Delete(string pipeline, string alias);
}

/// <summary><see cref="ICursorStore"/> in memory: the keeper's default when no store is given, and a test's.</summary>
public sealed class MemoryCursorStore : ICursorStore
{
    private readonly Dictionary<(string, string), string> _documents = [];

    public IReadOnlyList<StoredCursor> Load()
    {
        lock (_documents) return [.. _documents.Select(d => new StoredCursor(d.Key.Item1, d.Key.Item2, d.Value))];
    }

    public void Save(string pipeline, string alias, string document)
    {
        lock (_documents) _documents[(pipeline, alias)] = document;
    }

    public void Delete(string pipeline, string alias)
    {
        lock (_documents) _documents.Remove((pipeline, alias));
    }
}
