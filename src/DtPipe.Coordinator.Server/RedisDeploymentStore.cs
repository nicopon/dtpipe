using StackExchange.Redis;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="IDeploymentStore"/> on Redis: one hash at <c>{prefix}deployments</c>, a field per pipeline holding its document, so a
/// read gets every armed deployment in one command and a write touches one field. It borrows the coordinator's
/// <see cref="RedisConnection"/>.
/// </summary>
public sealed class RedisDeploymentStore(RedisConnection redis) : IDeploymentStore
{
    private readonly RedisKey _key = redis.KeyPrefix + "deployments";

    public IReadOnlyList<StoredDeployment> Load() => Call(() =>
        (IReadOnlyList<StoredDeployment>)[.. redis.Db.HashGetAll(_key).Select(e => new StoredDeployment(e.Name.ToString(), e.Value.ToString()))]);

    public void Save(string pipelineId, string document) => Call(() =>
    {
        redis.Db.HashSet(_key, pipelineId, document);
        return 0;
    });

    public void Delete(string pipelineId) => Call(() =>
    {
        redis.Db.HashDelete(_key, pipelineId);
        return 0;
    });

    private T Call<T>(Func<T> command) => redis.Call("the armed deployments", command);
}
