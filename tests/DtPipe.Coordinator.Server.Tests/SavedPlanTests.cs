using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

public sealed class SavedPlanTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "library-" + Guid.NewGuid().ToString("N"));

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private PipelineLibrary LibraryWith(string planJson)
    {
        var pipeline = Path.Combine(_dir, "library", "pipelines", "p");
        Directory.CreateDirectory(Path.Combine(pipeline, "distributed"));
        File.WriteAllText(Path.Combine(pipeline, "pipeline.yaml"), "# P\n");
        File.WriteAllText(Path.Combine(pipeline, "distributed", "plan.json"), planJson);
        return new PipelineLibrary(new CoordinatorServerOptions
        {
            StateDir = _dir, DtPipeExecutable = "dtpipe", Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
        });
    }

    private static string Plan(string fragmentGroups, string edgeGroups) => $$"""
        { "pipelineSha": "x", "savedAt": "2026-10-07T00:00:00+00:00", "plan": {
          "id": "1", "pipelineId": "p", "cuts": [], "units": [], "errors": [], "warnings": [], "flowRejection": null,
          "fragments": [ { "name": "p@a", "node": "a", {{fragmentGroups}} "yaml": "", "version": "v", "edges": [], "units": [] } ],
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
    public async Task A_Plan_Without_Groups_Is_Read_As_No_Plan()
    {
        var library = LibraryWith(Plan("\"group\": \"crm\",", "\"producerGroup\": \"crm\", \"consumerGroup\": \"runner\","));
        var document = await library.GetAsync("p", TestContext.Current.CancellationToken);
        Assert.Null(document!.Plan);
        Assert.False(document.PlanCurrent);
    }
}
