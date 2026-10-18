using StackExchange.Redis;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="IRightsStore"/> on Redis: one hash at <c>{prefix}rights</c> whose fields are <c>doc</c> (the document) and
/// <c>version</c>. Creating and replacing are scripts, so each is atomic and a replace compares the version it read. It borrows the
/// coordinator's <see cref="RedisConnection"/>.
/// </summary>
public sealed class RedisRightsStore(RedisConnection redis) : IRightsStore
{
    private const string CreateScript = """
        if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
        redis.call('HSET', KEYS[1], 'doc', ARGV[1], 'version', 1)
        return 1
        """;

    private const string ReplaceScript = """
        local version = redis.call('HGET', KEYS[1], 'version')
        if version == false or tonumber(version) ~= tonumber(ARGV[2]) then return -1 end
        local next = tonumber(version) + 1
        redis.call('HSET', KEYS[1], 'doc', ARGV[1], 'version', next)
        return next
        """;

    private readonly RedisKey _key = redis.KeyPrefix + "rights";

    public StoredRights? Load() => Call(() =>
    {
        var fields = redis.Db.HashGet(_key, ["doc", "version"]);
        return fields[0].HasValue && fields[1].HasValue ? new StoredRights(fields[0]!, (long)fields[1]) : null;
    });

    public bool Create(string document) => Call(() =>
    {
        if (!redis.Initialize)
            throw new InvalidOperationException(
                "The rights document is absent: Redis may have lost its data, and a rights document started again could grant what was revoked. "
                + "Set Coordinator:Redis:Initialize to true to begin new rights.");
        return (long)redis.Db.ScriptEvaluate(CreateScript, [_key], [document]) == 1;
    });

    public long? Replace(string document, long expectedVersion) => Call(() =>
    {
        var version = (long)redis.Db.ScriptEvaluate(ReplaceScript, [_key], [document, expectedVersion]);
        return version < 0 ? (long?)null : version;
    });

    private T Call<T>(Func<T> command) => redis.Call("the rights", command);
}
