using System.Xml;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection.Repositories;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Where the session cookie's keys are kept when the state is in SQLite: the <c>session_keys</c> table, one XML element per row,
/// appended and read whole (DataProtection keeps a handful, and an element is never changed). It borrows the coordinator's
/// <see cref="SqliteDatabase"/>. A key is a secret: no message here carries one, and a failure to read or write throws, never an empty
/// list, since DataProtection would then mint a new key and sign everyone out without saying so. The keys are stored as DataProtection
/// hands them over, unencrypted: the file is as readable as the account that owns it.
/// </summary>
public sealed class SqliteXmlRepository(SqliteDatabase database) : IXmlRepository
{
    public IReadOnlyCollection<XElement> GetAllElements() => database.Call("the session keys", (c, t) =>
    {
        try
        {
            return (IReadOnlyCollection<XElement>)[.. SqliteDatabase.Rows(c, t, "SELECT xml FROM session_keys ORDER BY id", r => XElement.Parse(r.GetString(0)))];
        }
        catch (XmlException)
        {
            // No inner exception: the parser's message may quote the key.
            throw new StoreUnavailableException("A session key kept in the database cannot be read.");
        }
    });

    public void StoreElement(XElement element, string friendlyName) => database.Call("the session keys", (c, t) =>
        SqliteDatabase.Run(c, t, "INSERT INTO session_keys (xml) VALUES ($xml)", ("$xml", element.ToString(SaveOptions.DisableFormatting))));
}
