using System.Text.Json;
using DtPipe.Cli.Mcp;
using DtPipe.Core.Abstractions;
using DtPipe.Sessions;
using DtPipe.Tests.Helpers;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// H7 — protocol-level tests, deterministic and without a model. A tool's contract is the shape
/// of what it returns; the agentic gate cannot tell a bad answer from a broken tool, so the
/// authoritative signal for these lives here.
/// </summary>
[Collection(SessionStateCollection.Name)]
public class McpCheckpointToolsTests : IDisposable
{
	private readonly string _tmp;
	private readonly string? _savedState;
	private readonly DtPipeMcpTools _tools;

	public McpCheckpointToolsTests()
	{
		_tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_mcpck_{Guid.NewGuid():N}");
		Directory.CreateDirectory(_tmp);
		_savedState = Environment.GetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable);
		Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, Path.Combine(_tmp, "state"));

		var services = new ServiceCollection();
		services.AddSingleton<DtPipe.Core.Options.OptionsRegistry>();
		services.AddSingleton<IEnumerable<IStreamTransformerFactory>>(Array.Empty<IStreamTransformerFactory>());
		services.AddSingleton<IEnumerable<IStreamReaderFactory>>(Array.Empty<IStreamReaderFactory>());
		services.AddSingleton<IEnumerable<IDataWriterFactory>>(Array.Empty<IDataWriterFactory>());
		services.AddSingleton<IMcpHelpService, McpHelpService>();
		var sp = services.BuildServiceProvider();
		_tools = new DtPipeMcpTools(
			sp.GetRequiredService<IEnumerable<IStreamReaderFactory>>(),
			Array.Empty<IDataTransformerFactory>(),
			sp.GetRequiredService<IEnumerable<IDataWriterFactory>>(),
			sp.GetRequiredService<IMcpHelpService>(),
			sp)
		{
			WorkingDirectoryOverride = _tmp
		};
	}

	public void Dispose()
	{
		Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, _savedState);
		if (Directory.Exists(_tmp)) Directory.Delete(_tmp, recursive: true);
	}

	private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

	[Fact]
	public void An_Empty_Session_Lists_Nothing_And_Is_Not_An_Error()
	{
		var result = Parse(_tools.ListCheckpoints("fresh"));

		result.GetProperty("success").GetBoolean().Should().BeTrue(
			"a working directory with nothing materialised yet is a normal state; reporting a fault would teach a model to treat it as failure");
		result.GetProperty("checkpoints").GetArrayLength().Should().Be(0);
	}

	[Fact]
	public async Task An_Unknown_Checkpoint_Returns_A_Structured_Error_And_What_Is_Available()
	{
		var result = Parse(await _tools.ReadCheckpoint("does-not-exist", session: "s1"));

		result.GetProperty("success").GetBoolean().Should().BeFalse();
		result.TryGetProperty("available", out _).Should().BeTrue("an error that does not say what would have worked wastes a turn");
		result.TryGetProperty("error", out _).Should().BeTrue();
	}

	[Fact]
	public async Task An_Empty_Key_Is_Refused_Rather_Than_Resolved()
	{
		var result = Parse(await _tools.ReadCheckpoint("  "));

		result.GetProperty("success").GetBoolean().Should().BeFalse();
	}

	[Fact]
	public async Task A_Materialised_Checkpoint_Is_Listed_And_Read_Back()
	{
		var session = SessionStore.Resolve("s1", _tmp);
		var store = new CheckpointStore(session);
		await store.WriteAsync("abc123", SampleBatches());

		var listed = Parse(_tools.ListCheckpoints("s1"));
		listed.GetProperty("checkpoints").EnumerateArray().Select(e => e.GetString()).Should().Contain("abc123");

		var read = Parse(await _tools.ReadCheckpoint("abc123", rows: 2, session: "s1"));
		read.GetProperty("success").GetBoolean().Should().BeTrue();
		read.GetProperty("columns").GetArrayLength().Should().Be(1);
		read.GetProperty("rows").GetArrayLength().Should().Be(2, "the row cap is honoured");
		read.GetProperty("rows")[0][0].GetString().Should().Be("0");
	}

	[Fact]
	public async Task The_Row_Cap_Is_Clamped_Rather_Than_Trusted()
	{
		var store = new CheckpointStore(SessionStore.Resolve("s1", _tmp));
		await store.WriteAsync("abc123", SampleBatches());

		var read = Parse(await _tools.ReadCheckpoint("abc123", rows: 999_999, session: "s1"));

		read.GetProperty("rows").GetArrayLength().Should().Be(4, "a model may ask for anything; the tool decides what it returns");
	}

	private static async IAsyncEnumerable<Apache.Arrow.RecordBatch> SampleBatches()
	{
		var schema = new Apache.Arrow.Schema.Builder()
			.Field(f => f.Name("Id").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(false)).Build();
		var b = new Apache.Arrow.Int32Array.Builder().AppendRange([0, 1, 2, 3]);
		yield return new Apache.Arrow.RecordBatch(schema, [b.Build()], 4);
		await Task.CompletedTask;
	}
}
