namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="IRunStore"/> in the coordinator's SQLite file: the <c>runs</c> table (a document, whether the run is over, and when it
/// ended), the <c>counters</c> table (the counter that draws ids) and the <c>owed</c> table (the abandonments, with when each falls
/// due). A write, and the cut of the history it triggers, are one transaction. It borrows the coordinator's <see cref="SqliteDatabase"/>.
/// </summary>
public sealed class SqliteRunStore(SqliteDatabase database) : IRunStore
{
    private const string Counter = "run";

    public long NextSequence() => database.Call("the run journal", (c, t) =>
        (long)SqliteDatabase.Scalar(c, t,
            "INSERT INTO counters (name, value) VALUES ($name, 1) ON CONFLICT (name) DO UPDATE SET value = value + 1 RETURNING value", ("$name", Counter))!);

    public void RaiseSequenceTo(long floor)
    {
        database.Call("the run journal", (c, t) =>
        {
            if (floor == 0 && !database.Initialize && SqliteDatabase.Scalar(c, t, "SELECT 1 FROM counters WHERE name = $name", ("$name", Counter)) is null)
                throw new InvalidOperationException(
                    "The run counter is absent and no run is kept: the database may have been lost, and run ids would start again. "
                    + "Set Coordinator:Initialize to true to begin a new journal.");
            return SqliteDatabase.Run(c, t,
                "INSERT INTO counters (name, value) VALUES ($name, $floor) ON CONFLICT (name) DO UPDATE SET value = MAX(value, excluded.value)",
                ("$name", Counter), ("$floor", floor));
        });
    }

    public void Put(StoredRun run) => database.Call("the run journal", (c, t) =>
    {
        SqliteDatabase.Run(c, t,
            "INSERT INTO runs (id, doc, is_over, at_ms) VALUES ($id, $doc, $over, $at) "
            + "ON CONFLICT (id) DO UPDATE SET doc = excluded.doc, is_over = excluded.is_over, at_ms = excluded.at_ms",
            ("$id", run.RunId), ("$doc", run.Document), ("$over", run.Over ? 1 : 0), ("$at", run.At.ToUnixTimeMilliseconds()));
        // The runs past the window go with their documents.
        if (run.Over)
            SqliteDatabase.Run(c, t,
                "DELETE FROM runs WHERE is_over = 1 AND id NOT IN (SELECT id FROM runs WHERE is_over = 1 ORDER BY at_ms DESC, id DESC LIMIT $window)",
                ("$window", IRunStore.Window));
        return 0;
    });

    public StoredRuns Load() => database.Call("the run journal", (c, t) => new StoredRuns(
        SqliteDatabase.Rows(c, t, "SELECT doc FROM runs WHERE is_over = 0 ORDER BY at_ms, id", r => r.GetString(0)),
        SqliteDatabase.Rows(c, t, "SELECT doc FROM runs WHERE is_over = 1 ORDER BY at_ms DESC, id DESC LIMIT $window", r => r.GetString(0), ("$window", IRunStore.Window))));

    public void AddOwed(IReadOnlyCollection<OwedFragment> owed, DateTimeOffset until) => database.Call("the run journal", (c, t) =>
    {
        foreach (var o in owed)
            SqliteDatabase.Run(c, t,
                "INSERT INTO owed (run_id, fragment, node, until_ms) VALUES ($run, $fragment, $node, $until) "
                + "ON CONFLICT (run_id, fragment, node) DO UPDATE SET until_ms = excluded.until_ms",
                ("$run", o.RunId), ("$fragment", o.Fragment), ("$node", o.Node), ("$until", until.ToUnixTimeMilliseconds()));
        return 0;
    });

    public IReadOnlyList<OwedFragment> ReadOwed(DateTimeOffset now) => database.Call("the run journal", (c, t) =>
    {
        SqliteDatabase.Run(c, t, "DELETE FROM owed WHERE until_ms <= $now", ("$now", now.ToUnixTimeMilliseconds()));
        return (IReadOnlyList<OwedFragment>)SqliteDatabase.Rows(c, t, "SELECT run_id, fragment, node FROM owed ORDER BY until_ms, run_id, fragment",
            r => new OwedFragment(r.GetString(0), r.GetString(1), r.GetString(2)));
    });

    public void RemoveOwed(OwedFragment owed) => database.Call("the run journal", (c, t) =>
        SqliteDatabase.Run(c, t, "DELETE FROM owed WHERE run_id = $run AND fragment = $fragment AND node = $node",
            ("$run", owed.RunId), ("$fragment", owed.Fragment), ("$node", owed.Node)));
}
