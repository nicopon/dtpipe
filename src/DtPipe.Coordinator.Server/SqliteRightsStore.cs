namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="IRightsStore"/> in the coordinator's SQLite file: one row of the <c>rights</c> table holding the document and its
/// version. Creating and replacing are single statements in a transaction, so each is atomic and a replace compares the version it
/// read. It borrows the coordinator's <see cref="SqliteDatabase"/>.
/// </summary>
public sealed class SqliteRightsStore(SqliteDatabase database) : IRightsStore
{
    public StoredRights? Load() => database.Call("the rights", (c, t) =>
        SqliteDatabase.Rows(c, t, "SELECT doc, version FROM rights WHERE id = 1", r => new StoredRights(r.GetString(0), r.GetInt64(1))).SingleOrDefault());

    public bool Create(string document)
    {
        if (!database.Initialize)
            throw new InvalidOperationException(
                "The rights document is absent: the database may have been lost, and a rights document started again could grant what was revoked. "
                + "Set Coordinator:Initialize to true to begin new rights.");
        return database.Call("the rights", (c, t) =>
            SqliteDatabase.Run(c, t, "INSERT OR IGNORE INTO rights (id, doc, version) VALUES (1, $doc, 1)", ("$doc", document)) == 1);
    }

    public long? Replace(string document, long expectedVersion) => database.Call("the rights", (c, t) =>
        SqliteDatabase.Run(c, t, "UPDATE rights SET doc = $doc, version = version + 1 WHERE id = 1 AND version = $version",
            ("$doc", document), ("$version", expectedVersion)) == 1 ? expectedVersion + 1 : (long?)null);
}
