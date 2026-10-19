namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="IDeploymentStore"/> in the coordinator's SQLite file: the <c>deployments</c> table, a row per pipeline holding its
/// document. It borrows the coordinator's <see cref="SqliteDatabase"/>.
/// </summary>
public sealed class SqliteDeploymentStore(SqliteDatabase database) : IDeploymentStore
{
    public IReadOnlyList<StoredDeployment> Load() => database.Call("the armed deployments", (c, t) =>
        (IReadOnlyList<StoredDeployment>)SqliteDatabase.Rows(c, t, "SELECT pipeline, doc FROM deployments", r => new StoredDeployment(r.GetString(0), r.GetString(1))));

    public void Save(string pipelineId, string document) => database.Call("the armed deployments", (c, t) =>
        SqliteDatabase.Run(c, t, "INSERT INTO deployments (pipeline, doc) VALUES ($id, $doc) ON CONFLICT (pipeline) DO UPDATE SET doc = excluded.doc",
            ("$id", pipelineId), ("$doc", document)));

    public void Delete(string pipelineId) => database.Call("the armed deployments", (c, t) =>
        SqliteDatabase.Run(c, t, "DELETE FROM deployments WHERE pipeline = $id", ("$id", pipelineId)));
}
