using System.Security.Claims;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

public sealed class RightsStoreTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "rights-" + Guid.NewGuid().ToString("N"));

    public RightsStoreTests() => Directory.CreateDirectory(_dir);

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static CoordinatorServerOptions Options(string dir, string? seed = null) => new()
    {
        StateDir = dir, Authority = "http://idp", Audience = "test",
        UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
    };

    private RightsStore StoreOf(string json)
    {
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, json);
        return MemoryRightsStore.Rights(Options(Path.Combine(_dir, "state-" + Guid.NewGuid().ToString("N")), seed));
    }

    private const string Document = """
        {
          "identities": [
            { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"] },
            { "clientId": "b", "name": "node-b", "description": "B", "groups": ["runner", "audit"] }
          ],
          "matrix": { "crm": ["runner", "audit"], "runner": ["warehouse"], "audit": ["warehouse"] }
        }
        """;

    private static IReadOnlySet<string> Set(params string[] groups) => new HashSet<string>(groups);

    [Fact]
    public void A_Node_Connects_Under_The_Client_Id_Of_Its_Identity()
    {
        var store = StoreOf(Document);
        Assert.Equal("a", store.SubjectOf("node-a"));
        Assert.Equal("b", store.SubjectOf("node-b"));
    }

    [Fact]
    public void A_Node_Without_An_Identity_Has_No_Subject()
    {
        var store = StoreOf(Document);
        var error = Assert.Throws<ConflictException>(() => store.SubjectOf("node-z"));
        Assert.Contains("node-z", error.Message);
    }

    [Fact]
    public void A_Renamed_Node_Is_Known_By_Its_New_Name_Only()
    {
        var store = StoreOf(Document);
        store.UpdateIdentity("a", "node-a2", "A", ["crm"], true);
        Assert.Equal("a", store.SubjectOf("node-a2"));
        Assert.Throws<ConflictException>(() => store.SubjectOf("node-a"));
    }

    [Fact]
    public void A_Disabled_Node_Has_No_Subject_And_No_Groups_Until_It_Is_Enabled_Again()
    {
        var store = StoreOf(Document);
        store.UpdateIdentity("a", "node-a", "A", ["crm"], false);

        var subject = Assert.Throws<ConflictException>(() => store.SubjectOf("node-a"));
        Assert.Contains("node-a", subject.Message);
        Assert.Contains("disabled", subject.Message);
        var groups = Assert.Throws<ConflictException>(() => store.GroupsOf("node-a"));
        Assert.Contains("disabled", groups.Message);
        Assert.Equal("b", store.SubjectOf("node-b"));

        store.UpdateIdentity("a", "node-a", "A", ["crm"], true);
        Assert.Equal("a", store.SubjectOf("node-a"));
        Assert.Equal(["crm"], store.GroupsOf("node-a"));
    }

    [Fact]
    public void A_Version_2_Document_Keeps_Its_Names_And_Groups()
    {
        var store = StoreOf(Document);
        Assert.Equal(["audit", "runner"], store.Find("b")!.Groups);
        Assert.Equal("node-a", store.Find("a")!.Name);
    }

    [Fact]
    public void Two_Identities_Cannot_Share_A_Node_Name()
    {
        var store = StoreOf(Document);
        var error = Assert.Throws<ConflictException>(() => store.UpdateIdentity("b", "node-a", "B", ["runner"], true));
        Assert.Contains("node-a", error.Message);
        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("c", "node-b", "", ["crm"])));
        Assert.Throws<ConflictException>(() => StoreOf(Document.Replace("\"node-b\"", "\"node-a\"")));
    }

    [Fact]
    public void An_Identity_Needs_A_Usable_Name_And_A_Group()
    {
        var store = StoreOf(Document);
        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("c", "Node C", "", ["crm"])));
        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("c", "node-c", "", [" ", ""])));
    }

    [Fact]
    public void Groups_Are_Trimmed_Sorted_And_Distinct()
    {
        var store = StoreOf(Document);
        store.AddIdentity(new Identity("c", "node-c", "", [" sales", "crm", "crm "]));
        Assert.Equal(["crm", "sales"], store.Find("c")!.Groups);
    }

    [Fact]
    public void A_Change_Is_Saved_As_Version_2_And_Read_Back()
    {
        var dir = Path.Combine(_dir, "state-saved");
        var seed = Path.Combine(_dir, "seed-saved.json");
        File.WriteAllText(seed, Document);
        MemoryRightsStore.Rights(Options(dir, seed)).UpdateIdentity("a", "node-a", "Renamed", ["crm", "sales"], true, "someone@test");
        var reread = MemoryRightsStore.Rights(Options(dir));
        Assert.Equal(["crm", "sales"], reread.Find("a")!.Groups);
        Assert.Equal("someone@test", reread.Snapshot().Audit![0].By);
    }

    [Fact]
    public void A_Store_That_Refuses_The_Write_Leaves_The_Rights_As_They_Were()
    {
        var seed = Path.Combine(_dir, "seed-refused.json");
        File.WriteAllText(seed, Document);
        var options = Options(Path.Combine(_dir, "state-refused"), seed);
        var failing = new FailingRightsStore(MemoryRightsStore.At(options.StateDir));
        var store = new RightsStore(failing, options);
        var before = store.Snapshot();

        failing.Down = true;
        Assert.Throws<StoreUnavailableException>(() => store.UpdateIdentity("a", null, null, ["sales"], null, "someone@test"));
        Assert.Throws<StoreUnavailableException>(() => store.SetMatrix([], "someone@test"));
        Assert.Throws<StoreUnavailableException>(() => store.RemoveIdentity("b"));

        Assert.Equal(["crm"], store.Find("a")!.Groups);
        Assert.NotNull(store.Find("b"));
        Assert.Equal(before.Matrix.Count, store.Snapshot().Matrix.Count);
        Assert.Equal(before.Audit!.Count, store.Snapshot().Audit!.Count);
        failing.Down = false;
        store.UpdateIdentity("a", null, null, ["sales"], null, "someone@test");
        Assert.Equal(["sales"], MemoryRightsStore.Rights(options).Find("a")!.Groups);
    }

    [Fact]
    public void A_Store_That_Moved_On_Is_Read_Again_And_The_Change_Is_Refused_Once()
    {
        var seed = Path.Combine(_dir, "seed-moved.json");
        File.WriteAllText(seed, Document);
        var options = Options(Path.Combine(_dir, "state-moved"), seed);
        var first = MemoryRightsStore.Rights(options);
        // Another writer kept a change this one never saw.
        var other = MemoryRightsStore.Rights(options);
        other.UpdateIdentity("b", null, "Changed elsewhere", null, null);

        Assert.Throws<ConflictException>(() => first.UpdateIdentity("a", null, "Mine", null, null));

        Assert.Equal("Changed elsewhere", first.Find("b")!.Description);
        Assert.Equal("A", first.Find("a")!.Description);
        first.UpdateIdentity("a", null, "Mine", null, null);
        Assert.Equal("Mine", MemoryRightsStore.Rights(options).Find("a")!.Description);
    }

    [Fact]
    public void Every_Group_Of_The_Sender_Must_Reach_Every_Group_Of_The_Receiver()
    {
        var store = StoreOf(Document);
        Assert.True(store.CanSend(Set("crm"), Set("runner")));
        Assert.True(store.CanSend(Set("crm"), Set("runner", "audit")));
        Assert.False(store.CanSend(Set("crm"), Set("runner", "warehouse")));
        Assert.True(store.CanSend(Set("runner", "audit"), Set("warehouse")));
        Assert.False(store.CanSend(Set("runner", "crm"), Set("warehouse")));
    }

    [Fact]
    public void A_Side_With_No_Group_Sends_And_Receives_Nothing()
    {
        var store = StoreOf(Document);
        Assert.False(store.CanSend(Set(), Set("runner")));
        Assert.False(store.CanSend(Set("crm"), Set()));
    }

    [Fact]
    public void The_Flow_Control_Reads_The_Matrix_As_It_Is_Now()
    {
        var store = StoreOf(Document);
        var flow = new RightsFlowControl(store);
        Assert.True(flow.CanSendTo(Set("crm"), Set("runner")));
        store.SetMatrix(new() { ["crm"] = ["audit"] });
        Assert.False(flow.CanSendTo(Set("crm"), Set("runner")));
    }

    private const string WithMonitor = """
        {
          "identities": [
            { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"] },
            { "clientId": "watch", "name": "supervisor", "description": "Reads /health", "kind": "monitor" }
          ],
          "matrix": {}
        }
        """;

    [Fact]
    public void An_Identity_Names_No_Kind_Is_A_Node()
    {
        var store = StoreOf(Document);

        Assert.All(store.Snapshot().Identities, i => Assert.Equal((IdentityKinds.Node, true), (i.Kind, i.IsNode)));
    }

    [Fact]
    public void A_Monitor_Belongs_To_No_Group_And_Its_Name_Is_No_Nodes()
    {
        var store = StoreOf(WithMonitor);

        var monitor = store.Find("watch")!;
        Assert.Equal((IdentityKinds.Monitor, false), (monitor.Kind, monitor.IsNode));
        Assert.Empty(monitor.Groups);
        Assert.Null(store.NodeIdentity("supervisor"));
        Assert.Equal("a", store.NodeIdentity("node-a")!.ClientId);
        Assert.Throws<ConflictException>(() => store.SubjectOf("supervisor"));
        Assert.Throws<ConflictException>(() => store.GroupsOf("supervisor"));
    }

    [Fact]
    public void A_Monitor_Keeps_No_Group_It_Is_Given_And_A_Node_Still_Needs_One()
    {
        var store = StoreOf(Document);

        store.AddIdentity(new Identity("watch", "supervisor", "", ["crm"], Kind: IdentityKinds.Monitor));

        Assert.Empty(store.Find("watch")!.Groups);
        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("c", "node-c", "", [])));
    }

    [Fact]
    public void A_Name_Belongs_To_One_Identity_Whatever_Its_Kind_And_A_Kind_Is_One_Of_The_Known()
    {
        var store = StoreOf(WithMonitor);

        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("c", "supervisor", "", ["crm"])));
        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("d", "node-d", "", ["crm"], Kind: "scheduler")));
    }

    private const string WithLauncher = """
        {
          "identities": [
            { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"] },
            { "clientId": "cron", "name": "scheduler", "description": "Nightly", "kind": "launcher", "pipelines": ["sales", "crm-copy", "sales"] },
            { "clientId": "idle", "name": "idle", "description": "Lists none", "kind": "launcher" }
          ],
          "matrix": {}
        }
        """;

    [Fact]
    public void A_Launcher_Belongs_To_No_Group_Lists_Its_Pipelines_Sorted_And_Is_No_Node()
    {
        var store = StoreOf(WithLauncher);

        var cron = store.Find("cron")!;
        Assert.Equal((IdentityKinds.Launcher, false), (cron.Kind, cron.IsNode));
        Assert.Empty(cron.Groups);
        Assert.Equal(["crm-copy", "sales"], cron.Pipelines);
        Assert.Null(store.NodeIdentity("scheduler"));
        Assert.Throws<ConflictException>(() => store.GroupsOf("scheduler"));
    }

    [Fact]
    public void A_Launcher_Launches_Only_The_Pipelines_It_Lists_And_One_That_Lists_None_Launches_None()
    {
        var store = StoreOf(WithLauncher);

        Assert.True(store.Find("cron")!.Launches("sales"));
        Assert.False(store.Find("cron")!.Launches("other"));
        Assert.Empty(store.Find("idle")!.Pipelines!);
        Assert.False(store.Find("idle")!.Launches("sales"));
        Assert.False(store.Find("a")!.Launches("sales"));
        store.UpdateIdentity("cron", null, null, null, enabled: false);
        Assert.False(store.Find("cron")!.Launches("sales"));
    }

    [Fact]
    public void Only_A_Launcher_Keeps_A_List_Of_Pipelines_And_An_Edit_That_Omits_It_Leaves_It()
    {
        var store = StoreOf(WithLauncher);

        store.AddIdentity(new Identity("watch", "supervisor", "", [], Kind: IdentityKinds.Monitor, Pipelines: ["sales"]));
        Assert.Empty(store.Find("watch")!.Pipelines!);
        store.UpdateIdentity("cron", null, "Weekly", null, null);
        Assert.Equal(["crm-copy", "sales"], store.Find("cron")!.Pipelines);
        store.UpdateIdentity("cron", null, null, null, null, "root@x.test", ["zeta", "alpha"]);
        Assert.Equal(["alpha", "zeta"], store.Find("cron")!.Pipelines);
        Assert.Contains("pipelines crm-copy+sales → alpha+zeta", store.Snapshot().Audit![0].Change);
        Assert.Throws<ConflictException>(() => store.UpdateIdentity("cron", null, null, null, null, null, ["Not A Pipeline"]));
        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("bad", "bad", "", [], Kind: IdentityKinds.Launcher, Pipelines: ["x y"])));
    }

    [Fact]
    public void A_Launcher_Is_Granted_No_Group_To_The_Data_Plane()
    {
        var store = StoreOf(WithLauncher);
        var provider = new RightsIdentityProvider(store);
        var now = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeSeconds().ToString();

        var token = new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim("sub", "cron"), new Claim("client_id", "cron"), new Claim("exp", now), new Claim("scope", "crm")], "test"));

        Assert.Empty(provider.ResolveIdentity(token, null, null)!.Groups);
    }

    [Fact]
    public void A_Monitor_Is_Granted_No_Group_To_The_Data_Plane()
    {
        var store = StoreOf(WithMonitor);
        var provider = new RightsIdentityProvider(store);
        var now = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeSeconds().ToString();

        ClaimsPrincipal Token(string client) => new(new ClaimsIdentity(
            [new Claim("sub", client), new Claim("client_id", client), new Claim("exp", now), new Claim("scope", "crm")], "test"));

        Assert.Equal(["crm"], provider.ResolveIdentity(Token("a"), null, null)!.Groups);
        Assert.Empty(provider.ResolveIdentity(Token("watch"), null, null)!.Groups);
    }
}
