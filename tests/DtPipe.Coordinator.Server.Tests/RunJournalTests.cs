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
    public void The_Journal_Keeps_The_Same_Window_At_Start_As_When_It_Records()
    {
        var journal = Open();
        var start = DateTimeOffset.UtcNow;
        for (var i = 1; i <= 205; i++)
            journal.Record(new RunView($"run-{i}", "plan", RunState.Succeeded, start.AddSeconds(i), start.AddSeconds(i), null, null, null, "p", start));

        var again = Open();

        Assert.Equal(200, again.List(limit: 1000).Count);
        Assert.Equal(200, journal.List(limit: 1000).Count);
        Assert.Null(again.Find(r => r.RunId == "run-5"));
        Assert.Equal("run-205", again.List(limit: 1).Single().RunId);
        Assert.Equal("run-206", again.NextRunId());
    }

    [Fact]
    public void The_Next_Id_Never_Goes_Back_Even_When_The_Run_That_Held_The_Highest_Fell_Out_Of_The_Window()
    {
        var journal = Open();
        for (var i = 0; i < 999; i++) journal.NextRunId();
        var start = DateTimeOffset.UtcNow;
        // The highest id started first: it is the oldest by start time, and the one the window drops.
        journal.Record(new RunView("run-999", "plan", RunState.Succeeded, start, start, null, null, null, "p", start));
        for (var i = 1; i <= 200; i++)
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
        Assert.Equal("run-9", Assert.Single(again.Orphans).RunId);
        Assert.Equal("run-10", again.NextRunId());
        // The store says the run is over: a second restart finds no orphan and ends nothing again.
        Assert.Empty(Open().Orphans);
    }

    [Fact]
    public void A_Running_Run_That_Never_Had_A_Plan_Is_Ended_But_Owes_Nothing_To_A_Node()
    {
        Open().Record(InFlight("run-1", RunState.Running));

        var again = Open();

        Assert.Equal(RunState.Error, Assert.Single(again.List()).State);
        Assert.Empty(again.Orphans);
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
