using StackExchange.Redis;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="IIgnoredHostStore"/> on Redis: a sorted set at <c>{prefix}ignored-hosts</c> whose members are the subjects and whose
/// scores are when each is let back in, in milliseconds since the epoch, so a read takes the members that score after now and the
/// bound on their number is a rank. Adding is a script, so the set never grows past <c>max</c> between two commands. It borrows the
/// coordinator's <see cref="RedisConnection"/>.
/// </summary>
public sealed class RedisIgnoredHostStore(RedisConnection redis) : IIgnoredHostStore
{
    private const string AddScript = """
        redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[3])
        redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
        redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[4]) + 1))
        return 1
        """;

    private readonly RedisKey _key = redis.KeyPrefix + "ignored-hosts";

    public IReadOnlyDictionary<string, DateTimeOffset> Load(DateTimeOffset now) => Call(() =>
        redis.Db.SortedSetRangeByScoreWithScores(_key, now.ToUnixTimeMilliseconds(), double.PositiveInfinity, Exclude.Start)
            .ToDictionary(e => e.Element.ToString(), e => DateTimeOffset.FromUnixTimeMilliseconds((long)e.Score), StringComparer.Ordinal));

    public void Add(string subject, DateTimeOffset until, DateTimeOffset now, int max) => Call(() =>
        redis.Db.ScriptEvaluate(AddScript, [_key], [subject, until.ToUnixTimeMilliseconds(), now.ToUnixTimeMilliseconds(), max]));

    private T Call<T>(Func<T> command) => redis.Call("the ignored hosts", command);
}
