using DtPipe.PipelineNode.Hosting;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

public class LabRequestsTests
{
    // A dtpipe that cannot start: a request that reaches it fails differently from a refusal.
    private static readonly DtPipeCommand Unreachable = new("/nonexistent/dtpipe", Path.GetTempPath(), new Dictionary<string, string>());

    private static readonly NodeVariable Warehouse = new("WAREHOUSE_DB", "/data/warehouse.duckdb", null, "duck");

    [Fact]
    public void Lab_mode_is_off_unless_the_host_turns_it_on() =>
        Assert.False(new DtPipeNodeOptions().LabMode);

    [Fact]
    public async Task A_node_whose_configuration_leaves_lab_mode_off_reads_nothing()
    {
        var answer = await new LabRequests("node-4", enabled: false, [Warehouse], Unreachable).QueryAsync("WAREHOUSE_DB", "SELECT 1", 10);

        Assert.Null(answer.Csv);
        Assert.Contains("LabMode off", answer.Error);
    }

    [Fact]
    public async Task Only_a_variable_given_an_engine_is_queried()
    {
        var secret = new NodeVariable("API_KEY", "s3cr3t", null, Engine: null);

        var answer = await new LabRequests("node-4", enabled: true, [Warehouse, secret], Unreachable).QueryAsync("API_KEY", "SELECT 1", 10);

        Assert.Null(answer.Csv);
        Assert.Contains("no dataset at API_KEY", answer.Error);
    }
}
