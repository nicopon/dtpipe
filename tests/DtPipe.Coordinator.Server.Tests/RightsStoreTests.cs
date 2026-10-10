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
        return new RightsStore(Options(Path.Combine(_dir, "state-" + Guid.NewGuid().ToString("N")), seed));
    }

    private const string Version2 = """
        {
          "version": 2,
          "identities": [
            { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"] },
            { "clientId": "b", "name": "node-b", "description": "B", "groups": ["runner", "audit"] }
          ],
          "matrix": { "crm": ["runner", "audit"], "runner": ["warehouse"], "audit": ["warehouse"] }
        }
        """;

    private static IReadOnlySet<string> Set(params string[] groups) => new HashSet<string>(groups);

    [Fact]
    public void A_Document_Without_Its_Version_Is_Refused()
    {
        var error = Assert.Throws<InvalidOperationException>(() => StoreOf("""{ "identities": [], "matrix": {} }"""));
        Assert.Contains("\"version\": 2", error.Message);
    }

    [Fact]
    public void A_Document_Of_Another_Version_Is_Refused()
    {
        var error = Assert.Throws<InvalidOperationException>(() => StoreOf("""{ "version": 1, "identities": [], "matrix": {} }"""));
        Assert.Contains("found 1", error.Message);
    }

    [Fact]
    public void A_Node_Connects_Under_The_Client_Id_Of_Its_Identity()
    {
        var store = StoreOf(Version2);
        Assert.Equal("a", store.SubjectOf("node-a"));
        Assert.Equal("b", store.SubjectOf("node-b"));
    }

    [Fact]
    public void A_Node_Without_An_Identity_Has_No_Subject()
    {
        var store = StoreOf(Version2);
        var error = Assert.Throws<ConflictException>(() => store.SubjectOf("node-z"));
        Assert.Contains("node-z", error.Message);
    }

    [Fact]
    public void A_Renamed_Node_Is_Known_By_Its_New_Name_Only()
    {
        var store = StoreOf(Version2);
        store.UpdateIdentity("a", "node-a2", "A", ["crm"], true);
        Assert.Equal("a", store.SubjectOf("node-a2"));
        Assert.Throws<ConflictException>(() => store.SubjectOf("node-a"));
    }

    [Fact]
    public void A_Version_2_Document_Keeps_Its_Names_And_Groups()
    {
        var store = StoreOf(Version2);
        Assert.Equal(["audit", "runner"], store.Find("b")!.Groups);
        Assert.Equal("node-a", store.Find("a")!.Name);
    }

    [Fact]
    public void Two_Identities_Cannot_Share_A_Node_Name()
    {
        var store = StoreOf(Version2);
        var error = Assert.Throws<ConflictException>(() => store.UpdateIdentity("b", "node-a", "B", ["runner"], true));
        Assert.Contains("node-a", error.Message);
        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("c", "node-b", "", ["crm"])));
        Assert.Throws<ConflictException>(() => StoreOf(Version2.Replace("\"node-b\"", "\"node-a\"")));
    }

    [Fact]
    public void An_Identity_Needs_A_Usable_Name_And_A_Group()
    {
        var store = StoreOf(Version2);
        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("c", "Node C", "", ["crm"])));
        Assert.Throws<ConflictException>(() => store.AddIdentity(new Identity("c", "node-c", "", [" ", ""])));
    }

    [Fact]
    public void Groups_Are_Trimmed_Sorted_And_Distinct()
    {
        var store = StoreOf(Version2);
        store.AddIdentity(new Identity("c", "node-c", "", [" sales", "crm", "crm "]));
        Assert.Equal(["crm", "sales"], store.Find("c")!.Groups);
    }

    [Fact]
    public void A_Change_Is_Saved_As_Version_2_And_Read_Back()
    {
        var dir = Path.Combine(_dir, "state-saved");
        var seed = Path.Combine(_dir, "seed-saved.json");
        File.WriteAllText(seed, Version2);
        new RightsStore(Options(dir, seed)).UpdateIdentity("a", "node-a", "Renamed", ["crm", "sales"], true, "someone@test");
        var reread = new RightsStore(Options(dir));
        Assert.Equal(["crm", "sales"], reread.Find("a")!.Groups);
        Assert.Equal("someone@test", reread.Snapshot().Audit![0].By);
    }

    [Fact]
    public void Every_Group_Of_The_Sender_Must_Reach_Every_Group_Of_The_Receiver()
    {
        var store = StoreOf(Version2);
        Assert.True(store.CanSend(Set("crm"), Set("runner")));
        Assert.True(store.CanSend(Set("crm"), Set("runner", "audit")));
        Assert.False(store.CanSend(Set("crm"), Set("runner", "warehouse")));
        Assert.True(store.CanSend(Set("runner", "audit"), Set("warehouse")));
        Assert.False(store.CanSend(Set("runner", "crm"), Set("warehouse")));
    }

    [Fact]
    public void A_Side_With_No_Group_Sends_And_Receives_Nothing()
    {
        var store = StoreOf(Version2);
        Assert.False(store.CanSend(Set(), Set("runner")));
        Assert.False(store.CanSend(Set("crm"), Set()));
    }

    [Fact]
    public void The_Flow_Control_Reads_The_Matrix_As_It_Is_Now()
    {
        var store = StoreOf(Version2);
        var flow = new RightsFlowControl(store);
        Assert.True(flow.CanSendTo(Set("crm"), Set("runner")));
        store.SetMatrix(new() { ["crm"] = ["audit"] });
        Assert.False(flow.CanSendTo(Set("crm"), Set("runner")));
    }
}
