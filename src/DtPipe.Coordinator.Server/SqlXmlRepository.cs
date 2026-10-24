using System.Xml;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection.Repositories;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// A shelf of XML elements in the coordinator's database: one table, one element per row, appended and read whole (a shelf keeps a
/// handful, and an element is never changed). The session cookie's keys are on <c>session_keys</c>, the key issuer's signing keys on
/// <c>issuer_keys</c> (<see cref="IssuerKeys"/>). It borrows the coordinator's <see cref="SqlDatabase"/>. What a shelf holds is a
/// secret: no message here carries one, and a failure to read or write throws, never an empty list, since DataProtection would then
/// mint a new key and sign everyone out without saying so. The session keys are stored as DataProtection hands them over,
/// unencrypted: the database is as readable as the account that owns it.
/// </summary>
internal sealed class SqlXmlRepository(SqlDatabase database, string table = "session_keys", string what = "session key") : IXmlRepository
{
    public IReadOnlyCollection<XElement> GetAllElements() => database.Call($"the {what}s", (c, t) =>
    {
        try
        {
            return (IReadOnlyCollection<XElement>)[.. SqlDatabase.Rows(c, t, $"SELECT xml FROM {table} ORDER BY id", r => XElement.Parse(r.GetString(0)))];
        }
        catch (XmlException)
        {
            // No inner exception: the parser's message may quote the key.
            throw new StoreUnavailableException($"A {what} kept in the database cannot be read.");
        }
    });

    public void StoreElement(XElement element, string friendlyName) => database.Call($"the {what}s", (c, t) =>
        SqlDatabase.Run(c, t, $"INSERT INTO {table} (xml) VALUES (@xml)", ("@xml", element.ToString(SaveOptions.DisableFormatting))));
}
