using StackExchange.Redis;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="ICursorStore"/> on Redis: one hash at <c>{prefix}cursors</c>, a field <c>pipeline/alias</c> per cursor holding its
/// document (a pipeline id holds no '/'), so a read gets every cursor in one command and a write touches one field. It borrows the
/// coordinator's <see cref="RedisConnection"/>.
/// </summary>
public sealed class RedisCursorStore(RedisConnection redis) : ICursorStore
{
    private readonly RedisKey _key = redis.KeyPrefix + "cursors";

    public IReadOnlyList<StoredCursor> Load() => Call(() =>
        (IReadOnlyList<StoredCursor>)[.. redis.Db.HashGetAll(_key).Select(e =>
        {
            var name = e.Name.ToString();
            var slash = name.IndexOf('/');
            return new StoredCursor(name[..slash], name[(slash + 1)..], e.Value.ToString());
        })]);

    public void Save(string pipeline, string alias, string document) => Call(() =>
    {
        redis.Db.HashSet(_key, $"{pipeline}/{alias}", document);
        return 0;
    });

    public void Delete(string pipeline, string alias) => Call(() =>
    {
        redis.Db.HashDelete(_key, $"{pipeline}/{alias}");
        return 0;
    });

    private T Call<T>(Func<T> command) => redis.Call("the cursors", command);
}
