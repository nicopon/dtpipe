using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Deployments;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A runner of the rights hosts no data and serves every group: the matrix allows the flows to and from its groups with no row, the
/// closure of origins still decides what crosses it, and the groups belong to runners alone.
/// </summary>
public sealed class RunnerFlowTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "runner-flow-" + Guid.NewGuid().ToString("N"));
    private readonly RightsStore _rights;

    public RunnerFlowTests()
    {
        Directory.CreateDirectory(_dir);
        _rights = MemoryRightsStore.Rights(new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
        });
        _rights.AddIdentity(new Identity("run-1", "r1", "", ["run"]) { Runner = true });
        _rights.AddIdentity(new Identity("shop", "shop", "", ["data"]));
    }

    public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

    private static HashSet<string> Set(params string[] groups) => [.. groups];

    [Fact]
    public void The_Groups_Of_A_Runner_Are_Open_Both_Ways_With_An_Empty_Matrix_And_No_Other_Is()
    {
        Assert.True(_rights.CanSend(Set("data"), Set("run")));
        Assert.True(_rights.CanSend(Set("run"), Set("data")));
        Assert.True(_rights.CanSend(Set("run"), Set("run")));
        Assert.False(_rights.CanSend(Set("data"), Set("audit")));
        Assert.False(_rights.CanSend(Set("data"), Set("data")));
        Assert.Empty(_rights.Snapshot().Matrix);
    }

    [Fact]
    public void What_Crosses_A_Runner_Still_Needs_Its_Origin_To_Reach_Its_Destination()
    {
        // The edge that leaves a runner carries the groups of what the runner read.
        Assert.False(_rights.CanSend(Set("run", "data"), Set("data")));

        _rights.AllowEdges([new GroupEdge("data", "data")]);

        Assert.True(_rights.CanSend(Set("run", "data"), Set("data")));
    }

    [Fact]
    public void An_Edge_To_Or_From_A_Runner_Writes_No_Row_Because_It_Is_Allowed_Already()
    {
        _rights.AllowEdges([new GroupEdge("data", "run"), new GroupEdge("run", "data")]);

        Assert.Empty(_rights.Snapshot().Matrix);
    }

    [Fact]
    public void A_Node_That_Is_No_Runner_Cannot_Hold_A_Group_Of_A_Runner_Whichever_Way_It_Comes_To_Do_So()
    {
        var added = Assert.Throws<ConflictException>(() => _rights.AddIdentity(new Identity("other", "other", "", ["run"])));
        Assert.Contains("Group 'run' belongs to runners", added.Message);

        var moved = Assert.Throws<ConflictException>(() => _rights.UpdateIdentity("shop", null, null, ["data", "run"], null));
        Assert.Contains("'shop' is no runner", moved.Message);

        _rights.AddIdentity(new Identity("shop-2", "shop-2", "", ["data"]));
        var made = Assert.Throws<ConflictException>(() => _rights.UpdateIdentity("shop", null, null, null, null, runner: true));
        Assert.Contains("'shop-2' is no runner", made.Message);

        _rights.AddIdentity(new Identity("run-2", "r2", "", ["run"]) { Runner = true });
        Assert.True(_rights.CanSend(Set("run"), Set("run")));
    }

    [Fact]
    public void A_Runner_Taken_Back_To_A_Node_Closes_Its_Groups()
    {
        _rights.UpdateIdentity("run-1", null, null, null, null, runner: false);

        Assert.False(_rights.CanSend(Set("data"), Set("run")));
        Assert.Contains("no longer a runner", _rights.Snapshot().Audit![0].Change);
    }

    [Fact]
    public void Only_A_Node_Can_Be_A_Runner()
    {
        var refused = Assert.Throws<ConflictException>(() => _rights.AddIdentity(new Identity("watch", "supervisor", "", [], Kind: IdentityKinds.Monitor) { Runner = true }));

        Assert.Contains("only a node can be a runner", refused.Message);
    }
}
