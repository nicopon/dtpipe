namespace DtPipe.Coordinator.Server;

/// <summary>The rights document as the store keeps it, and the version a write of it must name.</summary>
public sealed record StoredRights(string Document, long Version);

/// <summary>
/// What <see cref="RightsStore"/> keeps beyond the process: the rights as one serialized document. One coordinator writes it; the
/// version lets a write that another writer overtook, or that was acknowledged late, be told from one that follows the last. A store
/// holds the text, never the shape of the rights. A call that cannot reach the store throws <see cref="StoreUnavailableException"/>.
/// </summary>
public interface IRightsStore
{
    /// <summary>The document and its version, or null when none was ever written.</summary>
    StoredRights? Load();

    /// <summary>
    /// Writes <paramref name="document"/> as version 1 when none exists, and says whether it did: two starts that cross write one
    /// document. A store that may have lost its data refuses unless it was told to initialize.
    /// </summary>
    bool Create(string document);

    /// <summary>Replaces the document when the store still holds <paramref name="expectedVersion"/>; the new version, or null when it moved on.</summary>
    long? Replace(string document, long expectedVersion);
}
