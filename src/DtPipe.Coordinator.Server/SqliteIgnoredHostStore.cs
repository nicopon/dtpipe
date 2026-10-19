namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="IIgnoredHostStore"/> in the coordinator's SQLite file: the <c>ignored_hosts</c> table, a row per subject with the
/// instant it is let back in, in milliseconds since the epoch. Adding forgets what expired and keeps the <c>max</c> that are let back
/// in last, in one transaction. It borrows the coordinator's <see cref="SqliteDatabase"/>.
/// </summary>
public sealed class SqliteIgnoredHostStore(SqliteDatabase database) : IIgnoredHostStore
{
    public IReadOnlyDictionary<string, DateTimeOffset> Load(DateTimeOffset now) => database.Call("the ignored hosts", (c, t) =>
        SqliteDatabase.Rows(c, t, "SELECT subject, until_ms FROM ignored_hosts WHERE until_ms > $now",
            r => (Subject: r.GetString(0), Until: DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(1))), ("$now", now.ToUnixTimeMilliseconds()))
            .ToDictionary(e => e.Subject, e => e.Until, StringComparer.Ordinal));

    public void Add(string subject, DateTimeOffset until, DateTimeOffset now, int max) => database.Call("the ignored hosts", (c, t) =>
    {
        SqliteDatabase.Run(c, t, "DELETE FROM ignored_hosts WHERE until_ms <= $now", ("$now", now.ToUnixTimeMilliseconds()));
        SqliteDatabase.Run(c, t, "INSERT INTO ignored_hosts (subject, until_ms) VALUES ($subject, $until) ON CONFLICT (subject) DO UPDATE SET until_ms = excluded.until_ms",
            ("$subject", subject), ("$until", until.ToUnixTimeMilliseconds()));
        return SqliteDatabase.Run(c, t,
            "DELETE FROM ignored_hosts WHERE subject NOT IN (SELECT subject FROM ignored_hosts ORDER BY until_ms DESC, subject DESC LIMIT $max)", ("$max", max));
    });
}
