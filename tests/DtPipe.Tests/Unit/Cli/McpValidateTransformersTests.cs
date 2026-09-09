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

    /// <summary>
    /// Every wrong mapping at once. Reporting the first cost a round trip each: a recorded session
    /// spent five of eleven iterations on seven mappings, five of them wrong.
    /// </summary>
    [Fact]
    public void Every_Wrong_Faker_In_One_Block_Is_Reported_Together()
    {
        var json = _tools.ValidateYamlJob(@"
main:
  input: ""input.csv""
  output: ""out.csv""
  transformers:
    - type: fake
      mappings:
        a: ""date""
        b: ""firstName""
        c: ""commerce.brand""
");
        Assert.Contains("date.past", json);          // a — the dataset's methods
        Assert.Contains("Datasets:", json);          // b — no dataset at all
        Assert.Contains("commerce.product", json);   // c — the dataset's methods
    }

    /// <summary>The refusal names the tool that answers it — legal here, where that tool exists.</summary>
    [Fact]
    public void A_Faker_Refusal_Names_The_Tool_That_Lists_Them()
        => Assert.Contains("get-anonymization-help", _tools.ValidateYamlJob(string.Format(WithFaker, "date")));

    /// <summary>
    /// A verdict of "valid" is read as "done". It has to say what it did not look at: a recorded
    /// session delivered a plan this tool passed and the engine refused on the first column name.
    /// </summary>
    [Fact]
    public void The_Success_Says_What_It_Did_Not_Check_And_Names_Dry_Run()
    {
        var json = _tools.ValidateYamlJob(string.Format(WithFaker, "date.past"));

        Assert.Contains("notChecked", json);
        Assert.Contains("dry-run", json);
    }

    /// <summary>
    /// The shape it just built, beside the verdict. A recorded session wrote three branches whose
    /// foreign keys were Math.random() because nothing showed it the branches were independent.
    /// </summary>
    [Fact]
    public void The_Success_Reports_The_Shape_Of_The_Job()
    {
        var json = _tools.ValidateYamlJob(@"
products:
  input: ""input.csv""
  output: ""sqlite:Data Source=shop.db""
customers:
  input: ""input.csv""
  output: ""sqlite:Data Source=shop.db""
");
        Assert.Contains("\"branchesReadingAnother\": 0", json);
        Assert.Contains("\"independent\": 2", json);
        Assert.Contains("sharedTargets", json);
        Assert.Contains("shop.db", json);
    }

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

    /// <summary>
    /// A branch publishes a channel only when it has no output of its own, so a branch that writes
    /// to a target cannot also be read. Reaching the run instead died on "An Arrow channel with the
    /// alias 'x' is not registered" — an internal object, naming neither cause nor fix. Building two
    /// related tables is exactly the shape that hits it.
    /// </summary>
    [Fact]
    public void A_Branch_That_Writes_Cannot_Also_Be_Read()
    {
        var json = _tools.ValidateYamlJob(@"
products:
  input: ""input.csv""
  output: ""sqlite:Data Source=shop.db""
sales:
  input: ""input.csv""
  ref: [""products""]
  output: ""sqlite:Data Source=shop.db""
");
        Assert.Contains("\"success\": false", json);
        Assert.Contains("publishes nothing to read", json);
        Assert.Contains("from: products", json);
    }

    /// <summary>A branch with no output of its own is a legitimate source.</summary>
    [Fact]
    public void A_Branch_Without_An_Output_Can_Be_Read()
    {
        var json = _tools.ValidateYamlJob(@"
products:
  input: ""input.csv""
write:
  from: ""products""
  output: ""out.csv""
");
        Assert.DoesNotContain("publishes nothing to read", json);
        Assert.DoesNotContain("nothing to read", json);
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
