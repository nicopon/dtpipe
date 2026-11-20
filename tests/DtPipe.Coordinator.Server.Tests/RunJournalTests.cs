using System.Text.Json.Nodes;
using DtPipe.Coordinator.Server.Runs;
using DtPipe.Coordinator.Server.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

public sealed class RunJournalTests
{
    private readonly string _dir = "journal-" + Guid.NewGuid().ToString("N");

    private CoordinatorServerOptions Options() =>
        new() { StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };

    private RunJournal Open() => MemoryRunStore.Journal(Options());

    [Fact]
    public void The_Replica_A_Fragment_Ran_On_Is_Kept_With_The_Run()
    {
        var result = new RunResult(RunOutcome.Succeeded, null, [], new Dictionary<string, FragmentExitReport>(), [],
            Replicas: new Dictionary<string, RunReplica> { ["p@n"] = new("1a2b3c4d", "host:42") });
        Open().Record(new RunView("run-1", "plan-1", RunState.Succeeded, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, result, "ok", null, "p", DateTimeOffset.UtcNow));

        var again = Open();

        var replica = Assert.Single(Assert.Single(again.List()).Result!.Replicas!);
        Assert.Equal(("p@n", "1a2b3c4d", "host:42"), (replica.Key, replica.Value.Id, replica.Value.Origin));
    }

    /// <summary>
    /// The protection each end of an edge reported is kept with the run and read back, and a run kept before the field existed reads
    /// with both ends unknown: absent in the document, null in the record, never the level that seals nothing.
    /// </summary>
    [Fact]
    public void The_Protection_Each_End_Of_An_Edge_Reported_Is_Kept_With_The_Run_And_A_Run_Kept_Without_It_Reads_Unknown()
    {
        var result = new RunResult(RunOutcome.Succeeded, null, [], new Dictionary<string, FragmentExitReport>(),
            [new EdgeCount("p", "out", "c", "in", 5, 5, "encrypted-psk", null)]);
        Open().Record(new RunView("run-1", "plan-1", RunState.Succeeded, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, result, "ok", null, "p", DateTimeOffset.UtcNow));
        // The document of a run kept by a coordinator that had no such field: the same run, without it.
        var older = JsonNode.Parse(MemoryRunStore.At(_dir).Document("run-1")!)!;
        older["runId"] = "run-2";
        foreach (var edge in older["result"]!["edgeCounts"]!.AsArray())
        {
            edge!.AsObject().Remove("producerProtection");
            edge.AsObject().Remove("consumerProtection");
        }
        MemoryRunStore.At(_dir).Seed("run-2", older.ToJsonString(), over: true);

        var again = Open();

        var kept = Assert.Single(again.Find(r => r.RunId == "run-1")!.Result!.EdgeCounts);
        Assert.Equal(("encrypted-psk", null), (kept.ProducerProtection, kept.ConsumerProtection));
        var before = Assert.Single(again.Find(r => r.RunId == "run-2")!.Result!.EdgeCounts);
        Assert.Equal((5L, 5L, null, null), (before.Sent, before.Received, before.ProducerProtection, before.ConsumerProtection));
    }

    [Fact]
    public void Who_Launched_A_Run_And_The_Key_It_Came_With_Are_Kept_With_The_Run()
    {
        Open().Record(new RunView("run-1", "plan-1", RunState.Succeeded, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, null, "ok", null, "p",
            DateTimeOffset.UtcNow, LaunchedBy: "cron", IdempotencyKey: "night-1"));

        var again = Open();

        Assert.Equal("run-1", again.Find(r => r.LaunchedBy == "cron" && r.IdempotencyKey == "night-1")?.RunId);
        Assert.Null(again.Find(r => r.LaunchedBy == "other" && r.IdempotencyKey == "night-1"));
    }

    [Fact]
    public void The_Journal_Keeps_The_Latest_Ten_Runs_Of_Each_Pipeline_At_Start_As_When_It_Records()
    {
        var journal = Open();
        var start = DateTimeOffset.UtcNow;
        RunView Over(int i, string pipeline, RunState state = RunState.Succeeded) =>
            new($"run-{i}", "plan", state, start.AddSeconds(i), start.AddSeconds(i), null, null, null, pipeline, start);
        // One pipeline runs often and another seldom: the first pushes out its own history and leaves the second's whole.
        for (var i = 1; i <= RunJournal.KeptPerPipeline + 2; i++) journal.Record(Over(i, "busy"));
        for (var i = 13; i <= 14; i++) journal.Record(Over(i, "quiet"));
        journal.Record(Over(15, "quiet", RunState.Failed));

        var again = Open();

        Assert.Equal(RunJournal.KeptPerPipeline, journal.List("busy", 1000).Count);
        Assert.Equal(3, journal.List("quiet", 1000).Count);
        Assert.Equal(RunJournal.KeptPerPipeline, again.List("busy", 1000).Count);
        Assert.Equal(3, again.List("quiet", 1000).Count);
        Assert.Null(again.Find(r => r.RunId == "run-2"));
        Assert.Equal("run-12", again.List("busy", 1).Single().RunId);
        Assert.Equal(["quiet", "busy"], again.Latest().Select(r => r.PipelineId));
        // A page and its total are one pass: the total counts what the pipeline and the state match, before the limit cuts the page.
        var (succeeded, succeededTotal) = again.Page(null, 1, RunState.Succeeded);
        Assert.Equal(("run-14", RunJournal.KeptPerPipeline + 2), (Assert.Single(succeeded).RunId, succeededTotal));
        var (failed, failedTotal) = again.Page("quiet", 1000, RunState.Failed);
        Assert.Equal(("run-15", 1), (Assert.Single(failed).RunId, failedTotal));
        Assert.Equal(0, again.Page("busy", 1000, RunState.Failed).Total);
        Assert.Equal(RunJournal.KeptPerPipeline + 3, again.Page(null, 1).Total);
        Assert.Equal("run-16", again.NextRunId());
        // The store drops the documents with the runs the journal let go.
        Assert.Null(MemoryRunStore.At(_dir).Document("run-2"));
        Assert.NotNull(MemoryRunStore.At(_dir).Document("run-3"));
    }

    [Fact]
    public void The_Next_Id_Never_Goes_Back_Even_When_The_Run_That_Held_The_Highest_Fell_Out_Of_Its_Pipelines_History()
    {
        var journal = Open();
        for (var i = 0; i < 999; i++) journal.NextRunId();
        var start = DateTimeOffset.UtcNow;
        // The highest id started first: it is the oldest by start time, and the one its pipeline's history drops.
        journal.Record(new RunView("run-999", "plan", RunState.Succeeded, start, start, null, null, null, "p", start));
        for (var i = 1; i <= RunJournal.KeptPerPipeline; i++)
            journal.Record(new RunView($"run-{i}", "plan", RunState.Succeeded, start.AddSeconds(i), start.AddSeconds(i), null, null, null, "p", start));

        var again = Open();

        Assert.Null(again.Find(r => r.RunId == "run-999"));
        Assert.Equal("run-1000", again.NextRunId());
    }

    [Fact]
    public void The_Plan_A_Run_Ran_Is_Kept_With_The_Run()
    {
        var plan = new RunPlan(
            "plan-9", "runner-1", [new RunPlanFragment("p@n", "n", "v1"), new RunPlanFragment("p@runner-1", "runner-1", "v2")],
            [new RunPlanBinding("src", "n", "customers", "b1", "strict", BindingVerdicts.Exact)]);
        Open().Record(
            new RunView("run-1", "plan-9", RunState.Succeeded, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, null, null, null, "p", DateTimeOffset.UtcNow, plan));

        var back = Assert.Single(Open().List());

        Assert.Equal(plan.Runner, back.Plan!.Runner);
        Assert.Equal(plan.Fragments, back.Plan.Fragments);
        Assert.Equal(plan.Bindings, back.Plan.Bindings);
    }

    private static RunView InFlight(string id, RunState state, RunPlan? plan = null) =>
        new(id, "plan", state, DateTimeOffset.UtcNow, null, null, null, null, "p", DateTimeOffset.UtcNow, plan, LaunchedBy: "cron", IdempotencyKey: "k-" + id);

    [Fact]
    public void A_Run_In_Flight_Is_Stored_And_In_No_History()
    {
        var journal = Open();

        journal.Record(InFlight("run-1", RunState.Queued));

        Assert.NotNull(MemoryRunStore.At(_dir).Document("run-1"));
        Assert.Empty(journal.List());
        Assert.Null(journal.Find(r => r.RunId == "run-1"));
    }

    [Fact]
    public void A_Restart_Ends_A_Run_In_Flight_Keeps_Its_Key_And_Counts_Its_Id()
    {
        var journal = Open();
        journal.Record(InFlight("run-4", RunState.Queued));
        journal.Record(InFlight("run-9", RunState.Running, new RunPlan("plan", null, [new RunPlanFragment("p@n", "n", "v1")], [])));

        var again = Open();

        Assert.All(again.List(), r => Assert.Equal((RunState.Error, RunJournal.RestartedRefusal), (r.State, r.Refusal)));
        Assert.Equal(["run-4", "run-9"], again.List().Select(r => r.RunId).Order());
        Assert.NotNull(again.Find(r => r.IdempotencyKey == "k-run-9")!.FinishedAt);
        Assert.Equal(new OwedFragment("run-9", "p@n", "n"), Assert.Single(again.Owed));
        // Only the run that had started may have written a sink; the label is in the store, not only in memory.
        Assert.Equal([("run-4", false), ("run-9", true)], again.List().OrderBy(r => r.RunId).Select(r => (r.RunId, r.SinkMayBePartial)));
        Assert.True(Open().Find(r => r.RunId == "run-9")!.SinkMayBePartial);
        Assert.Equal("run-10", again.NextRunId());
        // The store says the run is over: a second restart ends nothing again, and the abandonment is still owed.
        Assert.Equal("run-9", Assert.Single(Open().Owed).RunId);
    }

    [Fact]
    public void A_Running_Run_That_Never_Had_A_Plan_Is_Ended_But_Owes_Nothing_To_A_Node()
    {
        Open().Record(InFlight("run-1", RunState.Running));

        var again = Open();

        Assert.Equal(RunState.Error, Assert.Single(again.List()).State);
        Assert.Empty(again.Owed);
    }

    [Fact]
    public void A_Run_That_Is_Over_Stays_In_History_When_The_Store_Cannot_Keep_It()
    {
        var store = new FailingRunStore(MemoryRunStore.At(_dir));
        var journal = new RunJournal(store, NullLogger<RunJournal>.Instance);
        store.Down = true;
        var over = InFlight("run-1", RunState.Succeeded) with { FinishedAt = DateTimeOffset.UtcNow };

        journal.Record(over);

        Assert.Equal("run-1", Assert.Single(journal.List()).RunId);
        Assert.Throws<StoreUnavailableException>(() => journal.Record(InFlight("run-2", RunState.Queued)));
        Assert.Throws<StoreUnavailableException>(() => journal.NextRunId());
    }
}
