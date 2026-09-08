using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Cli.Mcp;
using DtPipe.Core.Security;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

// ValidatePathSafety resolves against the process working directory, which another class in this
// collection moves into a temp folder while it runs.
[Collection(SessionStateCollection.Name)]
public class McpToolsTests
{
    private readonly ServiceProvider _serviceProvider;
    private readonly DtPipeMcpTools _tools;
    private readonly IMcpHelpService _helpService;

    public McpToolsTests()
    {
        var services = new ServiceCollection();
        // Silent logger: these tests exercise tool plumbing, not the F17 missing-options warning.
        services.AddSingleton(new DtPipe.Core.Options.OptionsRegistry(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DtPipe.Core.Options.OptionsRegistry>.Instance));
        services.AddSingleton<IEnumerable<IStreamTransformerFactory>>(Array.Empty<IStreamTransformerFactory>());

        var readerFactories = new IStreamReaderFactory[] { new DummyReaderFactory() };
        services.AddSingleton<IEnumerable<IStreamReaderFactory>>(readerFactories);
        services.AddSingleton<IEnumerable<IDataWriterFactory>>(Array.Empty<IDataWriterFactory>());

        _serviceProvider = services.BuildServiceProvider();

        _helpService = new McpHelpService(
            readerFactories,
            Array.Empty<IDataTransformerFactory>(),
            Array.Empty<IDataWriterFactory>());

        _tools = new DtPipeMcpTools(
            readerFactories,
            Array.Empty<IDataTransformerFactory>(),
            Array.Empty<IDataWriterFactory>(),
            _helpService,
            _serviceProvider);
    }

    private void InvokeValidatePathSafety(string path)
    {
        var method = typeof(DtPipeMcpTools).GetMethod("ValidatePathSafety", 
            BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
        Assert.NotNull(method);
        try
        {
            method.Invoke(null, new object?[] { path });
        }
        catch (TargetInvocationException ex)
        {
            throw ex.InnerException ?? ex;
        }
    }

    private string[] InvokeSplitArguments(string commandLine)
    {
        var method = typeof(DtPipeMcpTools).GetMethod("SplitArguments", 
            BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
        Assert.NotNull(method);
        return (string[])method.Invoke(null, new object[] { commandLine })!;
    }

    [Fact]
    public void ValidatePathSafety_PathWithinCwd_Success()
    {
        var relativePath = "data.csv";
        var nestedPath = Path.Combine("subfolder", "data.parquet");
        var absoluteInCwd = Path.Combine(Directory.GetCurrentDirectory(), "data.jsonl");

        // Should not throw
        InvokeValidatePathSafety(relativePath);
        InvokeValidatePathSafety(nestedPath);
        InvokeValidatePathSafety(absoluteInCwd);
    }

    [Fact]
    public void ValidatePathSafety_PathOutsideCwd_Throws()
    {
        var absoluteOutside = "/etc/passwd";
        var relativeParentEscaped = "../outside_cwd.csv";
        var complexEscaped = "subfolder/../../outside.csv";

        Assert.Throws<UnauthorizedAccessException>(() => InvokeValidatePathSafety(absoluteOutside));
        Assert.Throws<UnauthorizedAccessException>(() => InvokeValidatePathSafety(relativeParentEscaped));
        Assert.Throws<UnauthorizedAccessException>(() => InvokeValidatePathSafety(complexEscaped));
    }

    [Theory]
    [InlineData("Host=localhost;Database=mydb;Username=postgres;Password=123;")]
    [InlineData("Server=myServer;Database=db;User Id=uid;Password=pwd;")]
    [InlineData("sqlite:Host=dummy;Database=ignored;")]
    [InlineData("duck+mysql:Host=localhost;Database=mydb;User=root;")]
    [InlineData(":memory:")]
    [InlineData("-")]
    public void ValidatePathSafety_DbConnectionStringOrSpecial_SkipsCheck(string path)
    {
        // Should not throw even though it doesn't represent a valid file path inside CWD
        InvokeValidatePathSafety(path);
    }

    /// <summary>
    /// A blanket "StartsWith(duck+)" bypass used to exempt every hub connection string from path
    /// safety unconditionally, regardless of its content. Hub strings are relational connection
    /// strings (Host=/Database=/...), covered above, and get no special-cased exemption anymore —
    /// anything shaped like a workspace-escaping path is still checked the same way "duck:" is.
    /// </summary>
    [Fact]
    public void ValidatePathSafety_DuckHubPrefix_NoLongerBlanketBypassed()
    {
        Assert.Throws<UnauthorizedAccessException>(() => InvokeValidatePathSafety("duck+sqlite:/../../outside.db"));
    }

    [Fact]
    public void SplitArguments_SimpleAndQuotes_ParsedCorrectly()
    {
        var command = "dtpipe -i file.csv --sql \"SELECT * FROM table\" -o out.parquet";
        var expected = new[] { "dtpipe", "-i", "file.csv", "--sql", "SELECT * FROM table", "-o", "out.parquet" };

        var result = InvokeSplitArguments(command);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void McpSecurityContext_StateChange_Works()
    {
        IMcpSecurityContext context = new McpSecurityContext();
        Assert.False(context.IsMcpSession);
        
        context.IsMcpSession = true;
        Assert.True(context.IsMcpSession);
        
        context.IsMcpSession = false;
        Assert.False(context.IsMcpSession);
    }

    [Fact]
    public void Help_ReturnsGeneralHelpContent()
    {
        var result = _tools.Help();
        Assert.Contains("dtpipe — Data streaming & anonymization engine", result);
        Assert.Contains("YAML JOB USAGE", result);
        Assert.Contains("ADAPTERS:", result);
    }

    [Fact]
    public void ValidateYamlJob_EmptyYaml_ReturnsError()
    {
        var json = _tools.ValidateYamlJob("");
        Assert.Contains("YAML job content cannot be empty", json);
    }

    [Fact]
    public void ValidateYamlJob_ValidYaml_ReturnsSuccess()
    {
        var yaml = @"
main:
  input: ""csv:input.csv""
  output: ""csv:output.csv""
";
        var json = _tools.ValidateYamlJob(yaml);
        Assert.Contains("\"success\": true", json);
    }

    /// <summary>
    /// The validator must not be weaker than the engine. A recorded session invented a job shaped
    /// 'job: / sources: / transformers:' — which parses, because 'job' reads as a branch alias —
    /// was told it was valid, and spent the rest of its turn re-reading a plan the engine refuses
    /// before a row moves. A model with no error to act on has nothing to correct.
    /// </summary>
    [Fact]
    public void ValidateYamlJob_A_Branch_With_Nothing_To_Read_Is_Refused()
    {
        var yaml = @"
job:
  sources:
    - alias: customers
      type: generate
      rows: 2000
";
        var json = _tools.ValidateYamlJob(yaml);

        Assert.Contains("\"success\": false", json);
        Assert.Contains("nothing to read", json);
        Assert.Contains("job", json);
    }

    /// <summary>A branch fed by another through a stream processor reads through it, not through an
    /// 'input:' — refusing that would break every DAG the cookbook shows. The processor is stubbed
    /// here rather than pulled in: what the rule consults is whether one claims the branch.</summary>
    [Fact]
    public void ValidateYamlJob_A_Branch_Reading_Through_A_Processor_Is_Accepted()
    {
        var yaml = @"
src:
  input: ""csv:in.csv""
joined:
  from: ""src""
  provider-options:
    sql:
      query: ""SELECT * FROM src""
  output: ""csv:out.csv""
";
        Assert.Contains("\"success\": true", WithSqlProcessor().ValidateYamlJob(yaml));
    }

    /// <summary>The same tools, in a deployment where a stream processor named 'sql' is registered.</summary>
    private DtPipeMcpTools WithSqlProcessor()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new DtPipe.Core.Options.OptionsRegistry(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DtPipe.Core.Options.OptionsRegistry>.Instance));
        services.AddSingleton<IEnumerable<IStreamTransformerFactory>>(new IStreamTransformerFactory[] { new SqlProcessorStub() });
        var readers = new IStreamReaderFactory[] { new DummyReaderFactory() };
        services.AddSingleton<IEnumerable<IStreamReaderFactory>>(readers);
        services.AddSingleton<IEnumerable<IDataWriterFactory>>(Array.Empty<IDataWriterFactory>());

        return new DtPipeMcpTools(readers, Array.Empty<IDataTransformerFactory>(),
            Array.Empty<IDataWriterFactory>(), _helpService, services.BuildServiceProvider());
    }

    /// <summary>Claims a branch the way the real processors do — by its provider-options key. Only
    /// that question is asked during validation; nothing is ever created.</summary>
    private sealed class SqlProcessorStub : IStreamTransformerFactory
    {
        public string ComponentName => "sql";
        public string Category => "Stream Processors";
        public bool RequiresArrowChannels => true;
        public int MinStreams => 1;
        public int MaxStreams => 1;
        public int MinLookups => 0;
        public int MaxLookups => int.MaxValue;
        public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => new[] { ("--sql", false) };
        public bool IsApplicable(string[] branchArgs) => false;

        public IStreamTransformer Create(string[] branchArgs, DtPipe.Core.Pipelines.Dag.BranchChannelContext ctx, IServiceProvider sp)
            => throw new NotSupportedException("validation never creates one");

        public IStreamTransformer CreateFromJob(DtPipe.Core.Models.JobDefinition job, DtPipe.Core.Pipelines.Dag.BranchChannelContext ctx, IServiceProvider sp)
            => throw new NotSupportedException("validation never creates one");
    }

    /// <summary>
    /// A provider-option key that binds to nothing must reach the caller. It is otherwise the only
    /// mistake with no trace: the engine warns on stderr, which never enters a tool result, so the
    /// run keeps the default and reports success. The validator is where a caller comes to be told
    /// what is wrong before running.
    /// </summary>
    [Fact]
    public void ValidateYamlJob_An_Unknown_Provider_Option_Reaches_The_Caller()
    {
        var yaml = @"
main:
  input: ""input.csv""
  output: ""output.csv""
  provider-options:
    csv:
      no-such-option: 1
";
        var json = _tools.ValidateYamlJob(yaml);

        Assert.Contains("\"success\": false", json);
        Assert.Contains("no-such-option", json);
    }

    /// <summary>
    /// A block naming a provider this instance does not carry is left alone. Absence of a factory
    /// is not evidence of a wrong key, and a container assembled for one purpose does not register
    /// every provider — speaking there would condemn correct jobs.
    /// </summary>
    [Fact]
    public void ValidateYamlJob_A_Block_For_An_Absent_Provider_Is_Not_Judged()
    {
        var yaml = @"
main:
  input: ""input.csv""
  output: ""output.csv""
  provider-options:
    generate:
      anything-at-all: 1
";
        Assert.Contains("\"success\": true", _tools.ValidateYamlJob(yaml));
    }

    [Fact]
    public async System.Threading.Tasks.Task ExecuteYamlJob_EmptyYaml_ReturnsError()
    {
        var json = await _tools.ExecuteYamlJob("");
        Assert.Contains("YAML job content cannot be empty", json);
    }

    [Fact]
    public void GetAdapterHelp_UnknownAdapter_ReturnsError()
    {
        var json = _tools.GetAdapterHelp("nonexistent_adapter");
        Assert.Contains("Unknown adapter", json);
        Assert.Contains("nonexistent_adapter", json);
    }

    [Fact]
    public void GetTransformerHelp_UnknownTransformer_ReturnsError()
    {
        var json = _tools.GetTransformerHelp("nonexistent_transformer");
        Assert.Contains("Unknown transformer", json);
        Assert.Contains("nonexistent_transformer", json);
    }

    [Fact]
    public async System.Threading.Tasks.Task DryRun_InvalidYaml_ReturnsErrors()
    {
        var result = await _tools.DryRun("invalid_yaml_here");
        Assert.Contains("success\": false", result);
    }

    [Fact]
    public async System.Threading.Tasks.Task DryRun_ValidYamlNoProvider_ReturnsErrorsInBranches()
    {
        var yaml = @"
main:
  input: ""nonexistent_provider:dummy""
  output: ""csv:output.csv""
";
        var result = await _tools.DryRun(yaml);

        // The tool now runs the pipeline instead of describing it, so an unresolvable provider
        // is a failure rather than a success carrying a note — which is the more honest answer.
        // What must survive either way is the fail-closed marker.
        Assert.Contains("success\": false", result);
        Assert.Contains("\"applied\": false", result);
    }

    [Fact]
    public void GetDagTopology_EmptyYaml_ReturnsError()
    {
        var json = _tools.GetDagTopology("");
        Assert.Contains("YAML job content cannot be empty", json);
    }

    [Fact]
    public void GetDagTopology_MultiBranchDag_ReturnsStructuredBranches()
    {
        var yaml = @"
p:
  input: ""parquet:p.parquet""
c:
  input: ""csv:c.csv""
joined:
  from: p
  ref: [c]
  output: ""csv:out.csv""
";
        var json = _tools.GetDagTopology(yaml);

        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.True(root.GetProperty("success").GetBoolean());

        var branches = root.GetProperty("branches");
        Assert.Equal(3, branches.GetArrayLength());

        var joined = branches.EnumerateArray().Single(b => b.GetProperty("alias").GetString() == "joined");
        Assert.Equal("p", joined.GetProperty("from")[0].GetString());
        Assert.Equal("c", joined.GetProperty("ref")[0].GetString());
        Assert.Equal("csv:out.csv", joined.GetProperty("output").GetString());
    }

    [Fact]
    public void GetDagTopology_SanitisesConnectionStrings()
    {
        var yaml = @"
main:
  input: ""pg:Host=localhost;Username=admin;Password=s3cr3t""
  output: ""csv:out.csv""
";
        var json = _tools.GetDagTopology(yaml);
        Assert.DoesNotContain("s3cr3t", json);
    }

    [Fact]
    public void GetDagTopology_MalformedYaml_ReturnsErrorNotThrow()
    {
        var json = _tools.GetDagTopology("this: is: not: valid: yaml:");
        Assert.Contains("\"success\": false", json);
    }

    [Fact]
    public void ListCursors_NoStateFiles_ReturnsInfoMessage()
    {
        var result = _tools.ListCursors();
        Assert.Contains("No active cursor state files found", result);
    }

    [Fact]
    public async System.Threading.Tasks.Task SuggestPipeline_ValidSourceDest_GeneratesYaml()
    {
        var result = await _tools.SuggestPipeline("csv:input.csv", "sqlite:output.db");
        Assert.Contains("main:", result);
        Assert.Contains("input: \"csv:input.csv\"", result);
        Assert.Contains("output: \"sqlite:output.db\"", result);
    }

    /// <summary>
    /// The skeleton is the one thing this tool exists to produce, so every line of it must bind.
    /// It offered 'project' a 'columns:' list, which the deserializer drops: the transformer was
    /// built with an empty whitelist, passed every column through, and validated. A recorded
    /// session copied that block into all six candidates it wrote.
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task SuggestPipeline_The_Project_Block_It_Offers_Binds()
    {
        var skeleton = await _tools.SuggestPipeline("csv:input.csv", "sqlite:output.db");

        var uncommented = string.Join("\n", skeleton
            .Split('\n')
            .Select(l => System.Text.RegularExpressions.Regex.Replace(l, @"^(\s*)# ?", "$1"))
            .Where(l => !l.TrimStart().StartsWith("Source schema", StringComparison.Ordinal)
                     && !l.TrimStart().StartsWith("- id (", StringComparison.Ordinal)
                     && !l.TrimStart().StartsWith("- name (", StringComparison.Ordinal)
                     && !l.TrimStart().StartsWith("Uncomment", StringComparison.Ordinal)
                     && !l.TrimStart().StartsWith("(keys are", StringComparison.Ordinal)));

        var job = DtPipe.Configuration.JobFileParser.ParseContent(uncommented)["main"];
        var project = Assert.Single(job.Transformers!, t => t.Type == "project");

        var options = (DtPipe.Transformers.Arrow.Project.ProjectOptions)
            new DtPipe.Transformers.Arrow.Project.ProjectDataTransformerFactory(
                _serviceProvider.GetRequiredService<OptionsRegistry>())
            .CreateOptionsFromYaml(project)!;

        Assert.Equal(new[] { "id", "name" }, options.Project);
    }
}

public class DummyReaderFactory : IStreamReaderFactory
{
    public string ComponentName => "csv";
    public string Category => "Readers";
    public Type OptionsType => typeof(DtPipe.Core.Options.EmptyOptions);
    public bool RequiresQuery => false;
    public bool CanHandle(string connectionString) => connectionString.EndsWith(".csv");
    public IEnumerable<Type> GetSupportedOptionTypes() => new[] { typeof(DtPipe.Core.Options.EmptyOptions) };
    public IStreamReader Create(OptionsRegistry registry) => new DummyStreamReader();
}

public class DummyStreamReader : IStreamReader
{
    public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo>
    {
        new PipeColumnInfo("id", typeof(int), false),
        new PipeColumnInfo("name", typeof(string), true)
    };
    public System.Threading.Tasks.ValueTask DisposeAsync() => default;
    public Task OpenAsync(CancellationToken ct) => Task.CompletedTask;
    public IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, CancellationToken ct) => throw new NotImplementedException();
}
