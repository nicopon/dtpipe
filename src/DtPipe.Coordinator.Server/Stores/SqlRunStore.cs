using System.Data.Common;

namespace DtPipe.Coordinator.Server.Stores;

/// <summary>
/// <see cref="IRunStore"/> in the coordinator's database: the <c>runs</c> table (a document, whether the run is over, and when it
/// ended), the <c>counters</c> table (the counter that draws ids) and the <c>owed</c> table (the abandonments, with when each falls
/// due). A write, and the erasure of the runs it names, are one transaction. The counter has one writer, the coordinator that holds
/// the database, so it is read and written in two statements of one transaction. It borrows the coordinator's <see cref="SqlDatabase"/>.
/// </summary>
internal sealed class SqlRunStore(SqlDatabase database) : IRunStore
{
    private const string Counter = "run";
    private const string Erase = "DELETE FROM runs WHERE is_over = 1 AND id = @id";

    private readonly string _put = database.Dialect.Upsert("runs", ["id"], ["doc", "is_over", "at_ms"]);
    private readonly string _owe = database.Dialect.Upsert("owed", ["run_id", "fragment", "node"], ["until_ms"]);

    public long NextSequence() => database.Call("the run journal", (c, t) =>
    {
        if (SqlDatabase.Run(c, t, "UPDATE counters SET value = value + 1 WHERE name = @name", ("@name", Counter)) == 0)
            SqlDatabase.Run(c, t, "INSERT INTO counters (name, value) VALUES (@name, 1)", ("@name", Counter));
        return Convert.ToInt64(SqlDatabase.Scalar(c, t, "SELECT value FROM counters WHERE name = @name", ("@name", Counter)));
    });

    public void RaiseSequenceTo(long floor)
    {
        database.Call("the run journal", (c, t) =>
        {
            if (SqlDatabase.Scalar(c, t, "SELECT 1 FROM counters WHERE name = @name", ("@name", Counter)) is not null)
                return SqlDatabase.Run(c, t, "UPDATE counters SET value = @floor WHERE name = @name AND value < @floor", ("@name", Counter), ("@floor", floor));
            if (floor == 0 && !database.Initialize)
                throw new InvalidOperationException(
                    "The run counter is absent and no run is kept: the database may have been lost, and run ids would start again. "
                    + "Set Coordinator:Initialize to true to begin a new journal.");
            return SqlDatabase.Run(c, t, "INSERT INTO counters (name, value) VALUES (@name, @floor)", ("@name", Counter), ("@floor", floor));
        });
    }

    public void Put(StoredRun run, IReadOnlyCollection<string> forget) => database.Call("the run journal", (c, t) =>
    {
        SqlDatabase.Run(c, t, _put, ("@id", run.RunId), ("@doc", run.Document), ("@is_over", run.Over ? 1 : 0), ("@at_ms", run.At.ToUnixTimeMilliseconds()));
        Forget(c, t, forget);
        return 0;
    });

    public void Forget(IReadOnlyCollection<string> runIds) => database.Call("the run journal", (c, t) =>
    {
        Forget(c, t, runIds);
        return 0;
    });

    private static void Forget(DbConnection c, DbTransaction? t, IReadOnlyCollection<string> runIds)
    {
        foreach (var id in runIds) SqlDatabase.Run(c, t, Erase, ("@id", id));
    }

    public StoredRuns Load() => database.Call("the run journal", (c, t) => new StoredRuns(
        SqlDatabase.Rows(c, t, "SELECT doc FROM runs WHERE is_over = 0 ORDER BY at_ms, id", r => r.GetString(0)),
        SqlDatabase.Rows(c, t, "SELECT doc FROM runs WHERE is_over = 1 ORDER BY at_ms DESC, id DESC", r => r.GetString(0))));

    public void AddOwed(IReadOnlyCollection<OwedFragment> owed, DateTimeOffset until) => database.Call("the run journal", (c, t) =>
    {
        foreach (var o in owed)
            SqlDatabase.Run(c, t, _owe, ("@run_id", o.RunId), ("@fragment", o.Fragment), ("@node", o.Node), ("@until_ms", until.ToUnixTimeMilliseconds()));
        return 0;
    });

    public IReadOnlyList<OwedFragment> ReadOwed(DateTimeOffset now) => database.Call("the run journal", (c, t) =>
    {
        SqlDatabase.Run(c, t, "DELETE FROM owed WHERE until_ms <= @now", ("@now", now.ToUnixTimeMilliseconds()));
        return (IReadOnlyList<OwedFragment>)SqlDatabase.Rows(c, t, "SELECT run_id, fragment, node FROM owed ORDER BY until_ms, run_id, fragment",
            r => new OwedFragment(r.GetString(0), r.GetString(1), r.GetString(2)));
    });

    public void RemoveOwed(OwedFragment owed) => database.Call("the run journal", (c, t) =>
        SqlDatabase.Run(c, t, "DELETE FROM owed WHERE run_id = @run_id AND fragment = @fragment AND node = @node",
            ("@run_id", owed.RunId), ("@fragment", owed.Fragment), ("@node", owed.Node)));
}
