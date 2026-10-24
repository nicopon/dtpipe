namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="IIgnoredHostStore"/> in the coordinator's database: the <c>ignored_hosts</c> table, a row per subject with the
/// instant it is let back in, in milliseconds since the epoch. Adding forgets what expired and keeps the <c>max</c> that are let back
/// in last, in one transaction. It borrows the coordinator's <see cref="SqlDatabase"/>.
/// </summary>
internal sealed class SqlIgnoredHostStore(SqlDatabase database) : IIgnoredHostStore
{
    private readonly string _add = database.Dialect.Upsert("ignored_hosts", ["subject"], ["until_ms"]);
    private readonly string _bound =
        $"DELETE FROM ignored_hosts WHERE subject NOT IN (SELECT subject FROM ignored_hosts ORDER BY until_ms DESC, subject DESC {database.Dialect.First("@max")})";

    public IReadOnlyDictionary<string, DateTimeOffset> Load(DateTimeOffset now) => database.Call("the ignored hosts", (c, t) =>
        SqlDatabase.Rows(c, t, "SELECT subject, until_ms FROM ignored_hosts WHERE until_ms > @now",
            r => (Subject: r.GetString(0), Until: DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(1))), ("@now", now.ToUnixTimeMilliseconds()))
            .ToDictionary(e => e.Subject, e => e.Until, StringComparer.Ordinal));

    public void Add(string subject, DateTimeOffset until, DateTimeOffset now, int max) => database.Call("the ignored hosts", (c, t) =>
    {
        SqlDatabase.Run(c, t, "DELETE FROM ignored_hosts WHERE until_ms <= @now", ("@now", now.ToUnixTimeMilliseconds()));
        SqlDatabase.Run(c, t, _add, ("@subject", subject), ("@until_ms", until.ToUnixTimeMilliseconds()));
        return SqlDatabase.Run(c, t, _bound, ("@max", max));
    });
}
