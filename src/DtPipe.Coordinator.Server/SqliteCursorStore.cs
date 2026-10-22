namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="ICursorStore"/> in the coordinator's SQLite file: the <c>cursors</c> table, a row per (pipeline, sink alias) holding its
/// document. It borrows the coordinator's <see cref="SqliteDatabase"/>.
/// </summary>
public sealed class SqliteCursorStore(SqliteDatabase database) : ICursorStore
{
    public IReadOnlyList<StoredCursor> Load() => database.Call("the cursors", (c, t) =>
        (IReadOnlyList<StoredCursor>)SqliteDatabase.Rows(c, t, "SELECT pipeline, alias, doc FROM cursors", r => new StoredCursor(r.GetString(0), r.GetString(1), r.GetString(2))));

    public void Save(string pipeline, string alias, string document) => database.Call("the cursors", (c, t) =>
        SqliteDatabase.Run(c, t, "INSERT INTO cursors (pipeline, alias, doc) VALUES ($p, $a, $doc) ON CONFLICT (pipeline, alias) DO UPDATE SET doc = excluded.doc",
            ("$p", pipeline), ("$a", alias), ("$doc", document)));

    public void Delete(string pipeline, string alias) => database.Call("the cursors", (c, t) =>
        SqliteDatabase.Run(c, t, "DELETE FROM cursors WHERE pipeline = $p AND alias = $a", ("$p", pipeline), ("$a", alias)));
}
