using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Cli.Mcp;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Arrow.Fake;
using DtPipe.Transformers.Arrow.Project;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Services;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// <c>validate-yaml-job</c> answers one question — will this job run — and a validator weaker than
/// the engine answers it wrong in the direction that costs most: a caller told the job is fine has
/// nothing to correct.
///
/// <para>
/// Checking that a transformer's <c>type</c> is registered left everything a transformer rejects
/// at construction unchecked. A recorded session was told a three-branch job was valid, delivered
/// it as its plan, and the run died on <c>Unknown faker method 'date'</c> before a row moved.
/// </para>
/// </summary>
[Collection(SessionStateCollection.Name)]
public class McpValidateTransformersTests
{
    private readonly DtPipeMcpTools _tools;

    public McpValidateTransformersTests()
    {
        var registry = new OptionsRegistry();
        var js = new JsEngineProvider();
        var transformers = new IDataTransformerFactory[]
        {
            new FakeDataTransformerFactory(registry),
            new ProjectDataTransformerFactory(registry),
            new ComputeDataTransformerFactory(registry, js)
        };

        var services = new ServiceCollection();
        services.AddSingleton(registry);
        services.AddSingleton<IEnumerable<IDataTransformerFactory>>(transformers);
        services.AddSingleton<IEnumerable<IStreamTransformerFactory>>(Array.Empty<IStreamTransformerFactory>());
        var readers = new IStreamReaderFactory[] { new DummyReaderFactory() };
        services.AddSingleton<IEnumerable<IStreamReaderFactory>>(readers);
        services.AddSingleton<IEnumerable<IDataWriterFactory>>(Array.Empty<IDataWriterFactory>());
        var provider = services.BuildServiceProvider();

        _tools = new DtPipeMcpTools(
            readers,
            transformers,
            Array.Empty<IDataWriterFactory>(),
            new McpHelpService(readers, transformers, Array.Empty<IDataWriterFactory>()),
            provider);
    }

    private const string WithFaker = @"
main:
  input: ""input.csv""
  output: ""out.csv""
  transformers:
    - type: fake
      mappings:
        joined_date: ""{0}""
";

    /// <summary>The exact mapping a recorded session delivered as its validated plan.</summary>
    [Fact]
    public void A_Faker_Method_That_Does_Not_Exist_Is_Refused()
    {
        var json = _tools.ValidateYamlJob(string.Format(WithFaker, "date"));

        Assert.Contains("\"success\": false", json);
        Assert.Contains("Unknown faker method", json);
        Assert.Contains("Branch", json);
        Assert.Contains("main", json);
    }

    /// <summary>The refusal names the dataset's methods, so the next attempt has what it needs.</summary>
    [Fact]
    public void The_Refusal_Names_What_The_Dataset_Does_Have()
    {
        var json = _tools.ValidateYamlJob(string.Format(WithFaker, "date"));

        Assert.Contains("date.past", json);
        Assert.DoesNotContain("fake-list", json);
    }

    [Fact]
    public void A_Faker_Method_That_Exists_Passes()
        => Assert.Contains("\"success\": true", _tools.ValidateYamlJob(string.Format(WithFaker, "date.past")));

    /// <summary>An options block that builds no transformer is refused by the engine; so here.</summary>
    [Fact]
    public void A_Transformer_That_Builds_Nothing_Is_Refused()
    {
        var json = _tools.ValidateYamlJob(@"
main:
  input: ""input.csv""
  output: ""out.csv""
  transformers:
    - type: compute
      options:
        skip-null: true
");
        Assert.Contains("\"success\": false", json);
        Assert.Contains("produces no transformer", json);
    }

    [Fact]
    public void An_Unregistered_Type_Still_Names_The_Type()
    {
        var json = _tools.ValidateYamlJob(@"
main:
  input: ""input.csv""
  output: ""out.csv""
  transformers:
    - type: rename
      mappings:
        a: b
");
        Assert.Contains("Unknown transformer type", json);
        Assert.Contains("rename", json);
    }
}
