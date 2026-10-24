namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="ICursorStore"/> in the coordinator's database: the <c>cursors</c> table, a row per (pipeline, sink alias) holding its
/// document. It borrows the coordinator's <see cref="SqlDatabase"/>.
/// </summary>
internal sealed class SqlCursorStore(SqlDatabase database) : ICursorStore
{
    private readonly string _save = database.Dialect.Upsert("cursors", ["pipeline", "alias"], ["doc"]);

    public IReadOnlyList<StoredCursor> Load() => database.Call("the cursors", (c, t) =>
        (IReadOnlyList<StoredCursor>)SqlDatabase.Rows(c, t, "SELECT pipeline, alias, doc FROM cursors", r => new StoredCursor(r.GetString(0), r.GetString(1), r.GetString(2))));

    public void Save(string pipeline, string alias, string document) => database.Call("the cursors", (c, t) =>
        SqlDatabase.Run(c, t, _save, ("@pipeline", pipeline), ("@alias", alias), ("@doc", document)));

    public void Delete(string pipeline, string alias) => database.Call("the cursors", (c, t) =>
        SqlDatabase.Run(c, t, "DELETE FROM cursors WHERE pipeline = @pipeline AND alias = @alias", ("@pipeline", pipeline), ("@alias", alias)));
}
