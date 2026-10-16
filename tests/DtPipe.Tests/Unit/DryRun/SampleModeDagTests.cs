using DtPipe.Cli;
using AwesomeAssertions;
using System.Text.Json;
using DtPipe.Cli.Mcp;
using DtPipe.Sessions;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// A sample of a DAG is the real run with the writers that leave the process neutralised: the
/// channel between two branches carries its rows as it does in the real run. These sample through
/// the tool the studio and the agent call, on jobs where a branch reads another.
/// </summary>
[Collection(SessionStateCollection.Name)]
public sealed class SampleModeDagTests : IDisposable
{
	private readonly string _tmp;
	private readonly string _cwd;
	private readonly string? _savedState;

	public SampleModeDagTests()
	{
		_cwd = Directory.GetCurrentDirectory();
		_tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_sampledag_{Guid.NewGuid():N}");
		Directory.CreateDirectory(_tmp);
		_savedState = Environment.GetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable);
		Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, Path.Combine(_tmp, "state"));
		Directory.SetCurrentDirectory(_tmp);
	}

	public void Dispose()
	{
		Directory.SetCurrentDirectory(_cwd);
		Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, _savedState);
		try { Directory.Delete(_tmp, recursive: true); } catch { /* best effort */ }
	}

	private static JobAnalysis Tools()
	{
		var services = new ServiceCollection();
		Program.ConfigureServices(services);
		services.AddSingleton<IAnsiConsole>(AnsiConsole.Create(new AnsiConsoleSettings
		{
			Out = new AnsiConsoleOutput(new StringWriter()), Ansi = AnsiSupport.No, Interactive = InteractionSupport.No,
		}));
		return services.BuildServiceProvider().GetRequiredService<JobAnalysis>();
	}

	private static async Task<JsonElement> SampleAsync(string yaml, int rows)
	{
		var json = await Tools().SampleAsync(yaml, rows, shownRows: rows, CancellationToken.None);
		var root = JsonDocument.Parse(json).RootElement.Clone();
		Assert.True(root.GetProperty("success").GetBoolean(), json);
		return root;
	}

	private static JsonElement Branch(JsonElement report, string alias)
		=> report.GetProperty("branches").EnumerateArray().Single(b => b.GetProperty("branch").GetString() == alias);

	private static long[] Seen(JsonElement branch)
		=> branch.GetProperty("stages").EnumerateArray().Select(s => s.GetProperty("rowsSeen").GetInt64()).ToArray();

	private const string Chain = """
		a:
		  input: generate:100
		b:
		  from: a
		  transformers:
		  - type: filter
		    mappings:
		      row.GenerateIndex % 2 == 0: ''
		c:
		  from: b
		  transformers:
		  - type: compute
		    mappings:
		      x:row.GenerateIndex * 10: ''
		  output: csv:out.csv
		""";

	[Fact]
	public async Task A_Chain_Passes_Its_Rows_Past_The_First_Branch()
	{
		var report = await SampleAsync(Chain, rows: 10);

		Seen(Branch(report, "b")).Should().Equal([10L, 5L], "b reads the ten rows a read, and keeps the even ones");
		Seen(Branch(report, "c")).Should().Equal([5L, 5L], "c reads what b delivered, whole: only a source is bounded");
		File.Exists("out.csv").Should().BeFalse("the writer that leaves the process stays neutralised");
	}

	[Fact]
	public async Task A_Source_Is_Bounded_By_The_Sample_And_Nothing_Reads_More_Of_It()
	{
		var report = await SampleAsync(Chain, rows: 5);

		Seen(Branch(report, "a")).Should().Equal([5L], "the sample bounds the source and reads no more of it");
	}

	[Fact]
	public async Task A_Fan_Out_Gives_Each_Reader_The_Rows()
	{
		const string job = """
			a:
			  input: generate:50
			left:
			  from: a
			  output: csv:left.csv
			right:
			  from: a
			  output: csv:right.csv
			""";

		var report = await SampleAsync(job, rows: 6);

		Seen(Branch(report, "left"))[0].Should().Be(6);
		Seen(Branch(report, "right"))[0].Should().Be(6);
	}

	[Fact]
	public async Task Rows_Read_Is_What_The_Source_Delivered_For_A_Columnar_Reader()
	{
		var report = await SampleAsync("a:\n  input: generate:100\n  output: csv:out.csv\n", rows: 7);

		var branch = Branch(report, "a");
		branch.GetProperty("rowsRead").GetInt64().Should().Be(7);
		branch.GetProperty("rowsRead").GetInt64().Should().Be(Seen(branch)[0], "the reader stage counts the same rows");
	}

	[Fact]
	public async Task Rows_Read_Is_What_The_Source_Delivered_For_A_Row_Reader()
	{
		File.WriteAllText("people.csv", "id,name\n1,a\n2,b\n3,c\n");

		var report = await SampleAsync("a:\n  input: csv:people.csv\n  output: csv:out.csv\n", rows: 10);

		var branch = Branch(report, "a");
		branch.GetProperty("rowsRead").GetInt64().Should().Be(3);
		branch.GetProperty("rowsRead").GetInt64().Should().Be(Seen(branch)[0], "the reader stage counts the same rows");
	}

	[Fact]
	public async Task Two_Samples_At_Once_Each_Report_Only_Their_Own_Branch()
	{
		var tools = Tools();
		var big = tools.SampleAsync("alpha:\n  input: generate:200000\n  output: csv:o1.csv\n", 100000, shownRows: 3, CancellationToken.None);
		var small = tools.SampleAsync("beta:\n  input: generate:50\n  output: csv:o2.csv\n", 10, shownRows: 3, CancellationToken.None);

		var replies = await Task.WhenAll(big, small);

		var branches = replies
			.Select(r => JsonDocument.Parse(r).RootElement.GetProperty("branches").EnumerateArray()
				.Select(b => b.GetProperty("branch").GetString()).ToArray())
			.ToArray();
		branches[0].Should().Equal("alpha");
		branches[1].Should().Equal("beta");
	}

	[Fact]
	public async Task A_Processor_Reads_The_Rows_Of_Its_Upstream()
	{
		const string job = """
			a:
			  input: generate:20
			stream1:
			  from: a
			  provider-options:
			    sql:
			      query: SELECT * FROM a WHERE GenerateIndex % 2 = 0
			  output: csv:out.csv
			""";

		var report = await SampleAsync(job, rows: 10);

		Branch(report, "stream1").GetProperty("rowsDelivered").GetInt64().Should().Be(5);
	}

	[Fact]
	public async Task A_Checkpoint_Taken_In_A_Sample_Reads_Back()
	{
		var job = Chain.Replace("b:\n  from: a", "b:\n  from: a\n  checkpoint: s1");

		await SampleAsync(job, rows: 10);

		var store = new CheckpointStore(SessionStore.Resolve(null, _tmp));
		var rows = 0;
		foreach (var key in store.List())
			await foreach (var batch in store.ReadAsync(key))
				using (batch) rows += batch.Length;
		rows.Should().Be(5, "the checkpoint of b holds the rows b delivered");
	}

	[Fact]
	public async Task A_One_To_Many_Expansion_Arrives_Whole_At_The_Next_Branch()
	{
		const string job = """
			a:
			  input: generate:100
			s:
			  from: a
			  provider-options:
			    sql:
			      query: SELECT a.* FROM a, range(3)
			c:
			  from: s
			  output: csv:out.csv
			""";

		var report = await SampleAsync(job, rows: 5);

		Branch(report, "s").GetProperty("rowsDelivered").GetInt64().Should().Be(15, "the bound is the source's, applied once");
		Seen(Branch(report, "c"))[0].Should().Be(15, "c reads what s delivered, not five of it");
	}

	[Fact]
	public void A_Reader_Fed_By_Another_Stage_Is_Not_A_Source()
	{
		new CheckpointReaderFactory("abc", null).IsSource.Should().BeFalse(
			"a checkpoint holds rows an earlier run already bounded, so --dry-run N does not bound it again");

		var services = new ServiceCollection();
		Program.ConfigureServices(services);
		var readers = services.BuildServiceProvider().GetServices<DtPipe.Core.Abstractions.IStreamReaderFactory>().ToList();

		readers.Where(r => r.CapabilityKind is not null).Should().NotBeEmpty();
		readers.Where(r => r.CapabilityKind is not null).Should().OnlyContain(r => !r.IsSource,
			"a channel reader reads what the branch upstream produced");
		readers.Where(r => r.CapabilityKind is null).Should().OnlyContain(r => r.IsSource);
	}
}
