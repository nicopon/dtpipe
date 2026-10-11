using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

public sealed class SavedPlanTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "library-" + Guid.NewGuid().ToString("N"));

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    /// <summary>The groups the rights give each node now; a node absent from it has no active identity.</summary>
    private readonly Dictionary<string, IReadOnlyList<string>> _rights = [];

    private PipelineLibrary LibraryWith(string planJson)
    {
        var pipeline = Path.Combine(_dir, "library", "pipelines", "p");
        Directory.CreateDirectory(Path.Combine(pipeline, "distributed"));
        File.WriteAllText(Path.Combine(pipeline, "pipeline.yaml"), "# P\n");
        File.WriteAllText(Path.Combine(pipeline, "distributed", "plan.json"), planJson);
        return new PipelineLibrary(new CoordinatorServerOptions
        {
            StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
        }, node => _rights.GetValueOrDefault(node));
    }

    private static string Plan(string fragmentGroups, string edgeGroups, string form = "\"form\": \"Bricks\",", string sha = "x") => $$"""
        { "pipelineSha": "{{sha}}", "savedAt": "2026-10-07T00:00:00+00:00", "plan": {
          "id": "1", "pipelineId": "p", "cuts": [], "units": [], "errors": [], "warnings": [], "flowRejection": null,
          "fragments": [ { "name": "p@a", "node": "a", {{fragmentGroups}} {{form}} "yaml": "", "version": "v", "edges": [], "units": [] } ],
          "edges": [ { "producerFragment": "p@a", "producerAlias": "x", "consumerFragment": "p@b", "consumerAlias": "x", {{edgeGroups}} "allowed": true } ] } }
        """;

    [Fact]
    public async Task A_Plan_Written_With_Groups_Is_Read()
    {
        var library = LibraryWith(Plan("\"groups\": [\"crm\"],", "\"producerGroups\": [\"crm\"], \"consumerGroups\": [\"runner\"],"));
        var document = await library.GetAsync("p", TestContext.Current.CancellationToken);
        Assert.NotNull(document!.Plan);
        Assert.Equal(["crm"], document.Plan.Plan.Fragments[0].Groups);
    }

    [Fact]
    public async Task A_Plan_Whose_Fragments_Say_Not_How_They_Reach_Their_Node_Is_Read_As_No_Plan()
    {
        var library = LibraryWith(Plan("\"groups\": [\"crm\"],", "\"producerGroups\": [\"crm\"], \"consumerGroups\": [\"runner\"],", form: ""));
        var document = await library.GetAsync("p", TestContext.Current.CancellationToken);
        Assert.Null(document!.Plan);
    }

    [Fact]
    public async Task A_Plan_Without_Groups_Is_Read_As_No_Plan()
    {
        var library = LibraryWith(Plan("\"group\": \"crm\",", "\"producerGroup\": \"crm\", \"consumerGroup\": \"runner\","));
        var document = await library.GetAsync("p", TestContext.Current.CancellationToken);
        Assert.Null(document!.Plan);
        Assert.False(document.PlanCurrent);
    }

    private static readonly string CurrentSha = PipelineLibrary.Sha("# P\n");

    private PipelineLibrary LibraryWithCrmPlan()
    {
        var library = LibraryWith(Plan("\"groups\": [\"crm\", \"audit\"],", "\"producerGroups\": [\"crm\", \"audit\"], \"consumerGroups\": [\"runner\"],", sha: CurrentSha));
        return library;
    }

    /// <summary>A plan is current while its job is the one it was made from and each node holds the groups the plan names.</summary>
    [Fact]
    public async Task A_Plan_Is_Current_While_Its_Job_And_The_Groups_Of_Its_Nodes_Are_Unchanged()
    {
        var library = LibraryWithCrmPlan();
        _rights["a"] = ["crm", "audit"];

        var document = (await library.GetAsync("p", TestContext.Current.CancellationToken))!;
        Assert.True(document.PlanCurrent);
        Assert.Null(document.PlanStaleCause);
        Assert.True(Assert.Single(await library.ListAsync(TestContext.Current.CancellationToken)).PlanCurrent);
    }

    /// <summary>The groups of a node are a set: the order they are listed in is no change.</summary>
    [Fact]
    public async Task A_Plan_Stays_Current_When_A_Node_Lists_Its_Groups_In_Another_Order()
    {
        var library = LibraryWithCrmPlan();
        _rights["a"] = ["audit", "crm"];

        Assert.True((await library.GetAsync("p", TestContext.Current.CancellationToken))!.PlanCurrent);
    }

    [Fact]
    public async Task A_Plan_Is_Not_Current_When_A_Node_Holds_Other_Groups_Now()
    {
        var library = LibraryWithCrmPlan();
        _rights["a"] = ["crm"];

        var document = (await library.GetAsync("p", TestContext.Current.CancellationToken))!;
        Assert.False(document.PlanCurrent);
        Assert.Equal(PlanStaleCauses.Groups, document.PlanStaleCause);
        var listed = Assert.Single(await library.ListAsync(TestContext.Current.CancellationToken));
        Assert.False(listed.PlanCurrent);
        Assert.Equal(PlanStaleCauses.Groups, listed.PlanStaleCause);
    }

    /// <summary>A node with no identity, or a disabled one, can host nothing: a plan that places a fragment there is not current.</summary>
    [Fact]
    public async Task A_Plan_Is_Not_Current_When_A_Node_Has_No_Active_Identity()
    {
        var library = LibraryWithCrmPlan();

        var document = (await library.GetAsync("p", TestContext.Current.CancellationToken))!;
        Assert.False(document.PlanCurrent);
        Assert.Equal(PlanStaleCauses.Groups, document.PlanStaleCause);
        Assert.False(Assert.Single(await library.ListAsync(TestContext.Current.CancellationToken)).PlanCurrent);
    }

    [Fact]
    public async Task A_Plan_Made_From_Another_Job_Is_Not_Current_Whatever_The_Groups()
    {
        var library = LibraryWith(Plan("\"groups\": [\"crm\"],", "\"producerGroups\": [\"crm\"], \"consumerGroups\": [\"runner\"],", sha: "another-job"));
        _rights["a"] = ["crm"];

        var document = (await library.GetAsync("p", TestContext.Current.CancellationToken))!;
        Assert.False(document.PlanCurrent);
        Assert.Equal(PlanStaleCauses.Job, document.PlanStaleCause);
    }
}
