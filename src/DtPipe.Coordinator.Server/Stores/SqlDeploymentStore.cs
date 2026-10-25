namespace DtPipe.Coordinator.Server.Stores;

/// <summary>
/// <see cref="IDeploymentStore"/> in the coordinator's database: the <c>deployments</c> table, a row per pipeline holding its
/// document. It borrows the coordinator's <see cref="SqlDatabase"/>.
/// </summary>
internal sealed class SqlDeploymentStore(SqlDatabase database) : IDeploymentStore
{
    private readonly string _save = database.Dialect.Upsert("deployments", ["pipeline"], ["doc"]);

    public IReadOnlyList<StoredDeployment> Load() => database.Call("the armed deployments", (c, t) =>
        (IReadOnlyList<StoredDeployment>)SqlDatabase.Rows(c, t, "SELECT pipeline, doc FROM deployments", r => new StoredDeployment(r.GetString(0), r.GetString(1))));

    public void Save(string pipelineId, string document) => database.Call("the armed deployments", (c, t) =>
        SqlDatabase.Run(c, t, _save, ("@pipeline", pipelineId), ("@doc", document)));

    public void Delete(string pipelineId) => database.Call("the armed deployments", (c, t) =>
        SqlDatabase.Run(c, t, "DELETE FROM deployments WHERE pipeline = @pipeline", ("@pipeline", pipelineId)));
}
