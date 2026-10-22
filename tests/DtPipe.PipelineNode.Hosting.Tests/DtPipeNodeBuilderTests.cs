using System.Text;
using DtPipe.Coordinator.Contracts;
using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

public class DtPipeNodeBuilderTests
{
    private const string BrickBranch = """
        {
          "input": "sqlite:${{CRM_DB}}",
          "batch-size": 5000,
          "provider-options": {
            "sqlite-reader": { "query": "SELECT customer_id,\n  country\nFROM customers" }
          }
        }
        """;

    private static IConfiguration Json(string json) =>
        new ConfigurationBuilder().AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(json))).Build();

    private static NodeDefinition DefinitionOf(IServiceCollection services) =>
        services.BuildServiceProvider().GetRequiredService<NodeDefinition>();

    [Fact]
    public void A_brick_read_from_configuration_keeps_the_form_its_json_declares()
    {
        // A brick is matched by content: configuration turns every value into text and sorts keys,
        // and the coordinator must still recognise the branch the node's owner wrote.
        var file = Json($$"""{ "bricks": [ { "id": "countries", "kind": "source", "title": "Countries", "branch": {{BrickBranch}} } ] }""");
        var services = new ServiceCollection();
        services.AddDtPipeNode(_ => { }).AddBricks(file.GetSection("bricks"));

        var brick = Assert.Single(DefinitionOf(services).Bricks);
        Assert.Equal(BrickKind.Source, brick.Kind);
        Assert.Equal(BrickRules.Declared(BrickKind.Source, BrickBranch), BrickRules.Declared(brick.Kind, brick.BranchJson));
    }

    [Fact]
    public void Indexed_configuration_children_come_back_as_an_array()
    {
        var file = Json("""{ "bricks": [ { "id": "s", "kind": "Sink", "branch": { "output": "csv:out.csv", "keys": ["a", "b"] } } ] }""");
        var services = new ServiceCollection();
        services.AddDtPipeNode(_ => { }).AddBricks(file.GetSection("bricks"));

        var brick = Assert.Single(DefinitionOf(services).Bricks);
        Assert.Equal(BrickRules.Declared(BrickKind.Sink, """{ "output": "csv:out.csv", "keys": ["a", "b"] }"""),
                     BrickRules.Declared(brick.Kind, brick.BranchJson));
    }

    [Fact]
    public void A_brick_without_a_valid_kind_fails_where_it_is_declared()
    {
        var file = Json("""{ "bricks": [ { "id": "x", "kind": "transform", "branch": { "input": "csv:a.csv" } } ] }""");
        var builder = new ServiceCollection().AddDtPipeNode(_ => { });
        var error = Assert.Throws<InvalidOperationException>(() => builder.AddBricks(file.GetSection("bricks")));
        Assert.Contains("'x'", error.Message);
    }

    [Fact]
    public void A_branch_that_is_not_an_object_is_refused()
    {
        var builder = new ServiceCollection().AddDtPipeNode(_ => { });
        Assert.Throws<ArgumentException>(() => builder.AddBrick("x", BrickKind.Source, "x", "[1, 2]"));
    }

    [Fact]
    public void A_process_hosts_one_node()
    {
        var services = new ServiceCollection();
        services.AddDtPipeNode(_ => { });
        Assert.Throws<InvalidOperationException>(() => services.AddDtPipeNode(_ => { }));
    }

    [Fact]
    public void RunAnyFragment_overrides_the_configured_policy()
    {
        var services = new ServiceCollection();
        services.AddDtPipeNode(o => o.Policy = FragmentPolicy.Bricks).RunAnyFragment();
        Assert.Equal(FragmentPolicy.AnyFragment, DefinitionOf(services).Policy);
    }

    [Fact]
    public void A_host_registers_its_own_gate()
    {
        var services = new ServiceCollection();
        services.AddDtPipeNode(o => o.Name = "lab-1")
            .WithFragmentGate((_, _) => null);

        var definition = DefinitionOf(services);
        Assert.NotNull(definition.Gate);
    }

    [Fact]
    public void A_host_that_registers_nothing_has_no_gate()
    {
        var services = new ServiceCollection();
        services.AddDtPipeNode(o => o.Name = "node-1");

        var definition = DefinitionOf(services);
        Assert.Null(definition.Gate);
    }

    [Fact]
    public void The_options_bind_from_the_environment_shape()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["DtPipeNode:Name"] = "runner-1",
            ["DtPipeNode:CoordinatorUrl"] = "http://coordinator:8080",
            ["DtPipeNode:Policy"] = "AnyFragment",
            ["DtPipeNode:Oidc:Authority"] = "http://idp:5190",
            ["DtPipeNode:Oidc:ClientId"] = "runner-1",
            ["DtPipeNode:Oidc:ClientSecret"] = "secret",
            ["DtPipeNode:Variables:CRM_DB"] = "/data/crm.sqlite",
        }).Build();
        var services = new ServiceCollection();
        services.AddDtPipeNode(config.GetSection("DtPipeNode"));

        var options = services.BuildServiceProvider().GetRequiredService<IOptions<DtPipeNodeOptions>>().Value;
        Assert.Equal("runner-1", options.Name);
        Assert.Equal(FragmentPolicy.AnyFragment, options.Policy);
        Assert.Equal("http://idp:5190", options.Oidc.Authority);
        Assert.Equal("/data/crm.sqlite", options.Variables["CRM_DB"]);
    }

    [Fact]
    public async Task A_node_missing_its_settings_fails_at_start_naming_each_one()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDtPipeNode(o => o.CoordinatorUrl = "not a url");
        using var host = builder.Build();

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));
        Assert.Contains("DtPipeNode: Name is required.", error.Failures);
        Assert.Contains("DtPipeNode: CoordinatorUrl must be an absolute URL.", error.Failures);
        Assert.Contains("DtPipeNode: Oidc:ClientSecret is required.", error.Failures);
    }
}
