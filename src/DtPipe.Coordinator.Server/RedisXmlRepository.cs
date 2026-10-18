using System.Xml;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection.Repositories;
using StackExchange.Redis;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Where the session cookie's keys are kept: a list at <c>{prefix}session-keys</c>, one XML element per entry, appended and read whole
/// (DataProtection keeps a handful, and an element is never changed). It borrows the coordinator's <see cref="RedisConnection"/>.
/// A key is a secret: no message here carries one, and a failure to read or write throws, never an empty list, since DataProtection
/// would mint a new key and sign everyone out without saying so. The keys are stored as DataProtection hands them over, unencrypted,
/// as the directory they replace held them: a Redis that others can read can read them.
/// </summary>
public sealed class RedisXmlRepository(RedisConnection redis) : IXmlRepository
{
    private readonly RedisKey _key = redis.KeyPrefix + "session-keys";

    public IReadOnlyCollection<XElement> GetAllElements() => redis.Call("the session keys", () =>
    {
        try
        {
            return (IReadOnlyCollection<XElement>)[.. redis.Db.ListRange(_key).Select(entry => XElement.Parse(entry.ToString()))];
        }
        catch (XmlException)
        {
            // No inner exception: the parser's message may quote the key.
            throw new StoreUnavailableException("A session key kept in Redis cannot be read.");
        }
    });

    public void StoreElement(XElement element, string friendlyName) =>
        redis.Call("the session keys", () => redis.Db.ListRightPush(_key, element.ToString(SaveOptions.DisableFormatting)));
}
