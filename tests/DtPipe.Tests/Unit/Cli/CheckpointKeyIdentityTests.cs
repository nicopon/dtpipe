using AwesomeAssertions;
using DtPipe.Sessions;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A checkpoint key names what produces its rows. These run the real command line in process and
/// count the keys the store holds afterwards: <c>CheckpointKeyTests</c> proves <c>Compute</c>
/// tells apart what it is given, only the caller of production decides what it is given.
/// </summary>
[Collection(SessionStateCollection.Name)]
public class CheckpointKeyIdentityTests : IDisposable
{
	private readonly string _tmp;
	private readonly string _cwd;
	private readonly string? _savedState;

	public CheckpointKeyIdentityTests()
	{
		_cwd = Directory.GetCurrentDirectory();
		_tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_keyid_{Guid.NewGuid():N}");
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

	private CheckpointStore Store => new(SessionStore.Resolve(null, _tmp));

	private IReadOnlyList<string> Keys => Store.List();

	private async Task<int> RunAsync(params string[] args)
		=> await Program.Main(args);

	private async Task<int> RowsOfAsync(string key)
	{
		var rows = 0;
		await foreach (var batch in Store.ReadAsync(key))
		{
			using (batch) rows += batch.Length;
		}
		return rows;
	}

	[Fact]
	public async Task Two_Configurations_Of_One_Transformer_Keep_Two_Checkpoints()
	{
		(await RunAsync("-i", "generate:100", "--filter", "row.GenerateIndex < 10", "--checkpoint", "s1", "-o", "csv:a.csv")).Should().Be(0);
		(await RunAsync("-i", "generate:100", "--filter", "row.GenerateIndex < 50", "--checkpoint", "s1", "-o", "csv:b.csv")).Should().Be(0);

		var keys = Keys;
		keys.Should().HaveCount(2, "the filter's expression is part of what produces the rows");
		var sizes = new List<int>();
		foreach (var key in keys) sizes.Add(await RowsOfAsync(key));
		sizes.Should().BeEquivalentTo(new[] { 10, 50 });
	}

	[Fact]
	public async Task A_Sample_Never_Overwrites_The_Checkpoint_Of_A_Full_Run()
	{
		(await RunAsync("-i", "generate:100", "--compute", "x:row.GenerateIndex * 2", "--checkpoint", "s1", "-o", "csv:a.csv")).Should().Be(0);
		var full = Keys.Single();
		(await RowsOfAsync(full)).Should().Be(100);

		(await RunAsync("-i", "generate:100", "--compute", "x:row.GenerateIndex * 2", "--checkpoint", "s1", "--dry-run", "5", "-o", "csv:a.csv")).Should().Be(0);

		Keys.Should().HaveCount(2, "the effective limit of a sample is part of its key");
		(await RowsOfAsync(full)).Should().Be(100, "the full run's checkpoint keeps its rows");
	}

	[Fact]
	public async Task A_Branch_Reading_Another_Takes_Its_Upstream_Into_Its_Key()
	{
		string Job(int rows) => $"""
			src:
			  input: generate:{rows}
			t:
			  from: src
			  checkpoint: s1
			  transformers:
			  - type: compute
			    mappings:
			      x:row.GenerateIndex * 10: ''
			  output: csv:out.csv
			""";

		File.WriteAllText("a.yaml", Job(10));
		File.WriteAllText("b.yaml", Job(40));
		(await RunAsync("--job", "a.yaml")).Should().Be(0);
		(await RunAsync("--job", "b.yaml")).Should().Be(0);

		Keys.Should().HaveCount(2, "the branch's upstream is part of its prefix");
		var sizes = new List<int>();
		foreach (var key in Keys) sizes.Add(await RowsOfAsync(key));
		sizes.Should().BeEquivalentTo(new[] { 10, 40 });
	}

	[Fact]
	public async Task Renaming_An_Alias_Keeps_The_Key()
	{
		string Job(string alias) => $"""
			{alias}:
			  input: generate:10
			t:
			  from: {alias}
			  checkpoint: s1
			  transformers:
			  - type: compute
			    mappings:
			      x:row.GenerateIndex * 10: ''
			  output: csv:out.csv
			""";

		File.WriteAllText("a.yaml", Job("src"));
		File.WriteAllText("b.yaml", Job("origin"));
		(await RunAsync("--job", "a.yaml")).Should().Be(0);
		(await RunAsync("--job", "b.yaml")).Should().Be(0);

		Keys.Should().HaveCount(1, "an alias is a label, and labels are not identity");
	}

	[Fact]
	public async Task Two_Queries_Over_One_Upstream_Keep_Two_Checkpoints()
	{
		string Job(string where) => $"""
			a:
			  input: generate:20
			stream1:
			  from: a
			  checkpoint: s1
			  provider-options:
			    sql:
			      query: SELECT * FROM a WHERE {where}
			  output: csv:out.csv
			""";

		File.WriteAllText("a.yaml", Job("GenerateIndex % 2 = 0"));
		File.WriteAllText("b.yaml", Job("GenerateIndex % 4 = 0"));
		(await RunAsync("--job", "a.yaml")).Should().Be(0);
		(await RunAsync("--job", "b.yaml")).Should().Be(0);

		Keys.Should().HaveCount(2, "a processor's query is part of what produces its rows");
	}

	[Fact]
	public async Task The_Same_Pipeline_Twice_Reuses_Its_Checkpoint()
	{
		(await RunAsync("-i", "generate:20", "--filter", "row.GenerateIndex < 5", "--checkpoint", "s1", "-o", "csv:a.csv")).Should().Be(0);
		(await RunAsync("-i", "generate:20", "--filter", "row.GenerateIndex < 5", "--checkpoint", "s1", "-o", "csv:b.csv")).Should().Be(0);

		Keys.Should().HaveCount(1, "an unchanged prefix is reused");
	}
}
