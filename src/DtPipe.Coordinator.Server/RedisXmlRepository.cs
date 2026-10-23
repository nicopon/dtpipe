using System.Xml;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection.Repositories;
using StackExchange.Redis;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// A shelf of XML elements in Redis: a list, one element per entry, appended and read whole (a shelf keeps a handful, and an element is
/// never changed). The session cookie's keys are at <c>{prefix}session-keys</c>, the key issuer's signing keys at
/// <c>{prefix}issuer-keys</c> (<see cref="IssuerKeys"/>). It borrows the coordinator's <see cref="RedisConnection"/>. What a shelf holds
/// is a secret: no message here carries one, and a failure to read or write throws, never an empty list, since DataProtection would
/// mint a new key and sign everyone out without saying so. The session keys are stored as DataProtection hands them over, unencrypted,
/// as the directory they replace held them: a Redis that others can read can read them.
/// </summary>
public sealed class RedisXmlRepository(RedisConnection redis, string list = "session-keys", string what = "session key") : IXmlRepository
{
    private readonly RedisKey _key = redis.KeyPrefix + list;

    public IReadOnlyCollection<XElement> GetAllElements() => redis.Call($"the {what}s", () =>
    {
        try
        {
            return (IReadOnlyCollection<XElement>)[.. redis.Db.ListRange(_key).Select(entry => XElement.Parse(entry.ToString()))];
        }
        catch (XmlException)
        {
            // No inner exception: the parser's message may quote the key.
            throw new StoreUnavailableException($"A {what} kept in Redis cannot be read.");
        }
    });

    public void StoreElement(XElement element, string friendlyName) =>
        redis.Call($"the {what}s", () => redis.Db.ListRightPush(_key, element.ToString(SaveOptions.DisableFormatting)));
}
