using System.Text.Json;
using DtPipe.Cli.Mcp;
using DtPipe.Core.Abstractions;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A query passed to an MCP tool has to reach the reader.
///
/// It did not. OptionsRegistry.Get returns a THROWAWAY instance when the type was never
/// registered — over MCP that is every provider, since nothing binds CLI flags there — so the
/// tool set Query on an object nobody would read again, the factory built a fresh default, and
/// the reader answered "a query is required" to a call that had supplied one. `inspect` and
/// `preview-data` were unusable against SQLite, DuckDB and every ADO provider.
///
/// It survived because nothing exercised it: the unit tests called the C# methods with providers
/// whose options happened to be registered, and the agentic missions — the one thing that would
/// have caught it — had no corpus. Running them found it in twenty-five iterations of a model
/// trying the same call five different ways.
///
/// In the shared collection because ValidatePathSafety reads the process-wide current
/// directory, which another class in that collection moves.
/// </summary>
[Collection(DtPipe.Tests.Helpers.SessionStateCollection.Name)]
public class McpQueryPassthroughTests
{
	private static DtPipeMcpTools BuildTools()
	{
		var services = new ServiceCollection();
		services.AddLogging();
		services.AddSingleton<DtPipe.Core.Options.OptionsRegistry>();
		services.AddSingleton<IEnumerable<IStreamTransformerFactory>>(Array.Empty<IStreamTransformerFactory>());
		// Wrapped exactly as Program.cs wraps every descriptor, so the registry interaction under
		// test is the real one and not a simplified stand-in.
		services.AddSingleton<IEnumerable<IStreamReaderFactory>>(sp => new IStreamReaderFactory[]
		{
			new DtPipe.Cli.Infrastructure.CliStreamReaderFactory(
				new DtPipe.Adapters.Sqlite.SqliteReaderDescriptor(),
				sp.GetRequiredService<DtPipe.Core.Options.OptionsRegistry>(), sp),
			// A reader that takes no query, so the other half of the rule is exercised too.
			new DtPipe.Cli.Infrastructure.CliStreamReaderFactory(
				new DtPipe.Adapters.Csv.CsvReaderDescriptor(),
				sp.GetRequiredService<DtPipe.Core.Options.OptionsRegistry>(), sp)
		});
		services.AddSingleton<IEnumerable<IDataWriterFactory>>(Array.Empty<IDataWriterFactory>());
		services.AddSingleton<IMcpHelpService, McpHelpService>();
		var sp = services.BuildServiceProvider();
		return new DtPipeMcpTools(
			sp.GetRequiredService<IEnumerable<IStreamReaderFactory>>(),
			Array.Empty<IDataTransformerFactory>(),
			sp.GetRequiredService<IEnumerable<IDataWriterFactory>>(),
			sp.GetRequiredService<IMcpHelpService>(),
			sp);
	}

	[Fact]
	public async Task A_Query_Given_To_Inspect_Reaches_The_Reader()
	{
		// Inside the working directory on purpose: the MCP tools refuse a path outside it
		// (ValidatePathSafety), which is what a real caller is subject to as well.
		var db = Path.Combine(Directory.GetCurrentDirectory(), $"dtpipe_q_{Guid.NewGuid():N}.db");
		try
		{
			await using (var conn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={db}"))
			{
				await conn.OpenAsync();
				var cmd = conn.CreateCommand();
				cmd.CommandText = "CREATE TABLE t (id INTEGER, label TEXT); INSERT INTO t VALUES (1,'a'),(2,'b');";
				await cmd.ExecuteNonQueryAsync();
			}

			var json = await BuildTools().Inspect($"sqlite:Data Source={db}", "SELECT id, label FROM t");

			json.Should().NotContain("A query is required",
				"the query was supplied; reporting it as missing means it was dropped between the tool and the reader");
			JsonDocument.Parse(json).RootElement.ToString().Should().Contain("label");
		}
		finally
		{
			if (File.Exists(db)) File.Delete(db);
		}
	}

	/// <summary>
	/// The write-back above is guarded by <c>readerOpts is IQueryAwareOptions</c>, and that test
	/// had no else: a query aimed at a reader carrying none was dropped, and the tool answered
	/// about the whole source as though it had been narrowed. Silence there is worse than over
	/// the CLI, because the caller is a model with no other way to notice.
	/// </summary>
	[Theory]
	[InlineData("inspect")]
	[InlineData("preview-data")]
	public async Task A_Query_Given_To_A_Reader_That_Takes_None_Is_Refused(string tool)
	{
		var csv = Path.Combine(Directory.GetCurrentDirectory(), $"dtpipe_q_{Guid.NewGuid():N}.csv");
		try
		{
			await File.WriteAllTextAsync(csv, "id,label\n1,a\n2,b\n");
			var tools = BuildTools();

			var json = tool == "inspect"
				? await tools.Inspect($"csv:{csv}", "SELECT id FROM t")
				: await tools.PreviewData($"csv:{csv}", "SELECT id FROM t");

			var error = JsonDocument.Parse(json).RootElement.GetProperty("error").GetString();
			error.Should().Contain("takes no query",
				"a query that binds to nothing has to be named, not dropped");
			error.Should().Contain("get-adapter-help",
				"the refusal points at the tool that lists what the reader does accept");
		}
		finally
		{
			if (File.Exists(csv)) File.Delete(csv);
		}
	}

	/// <summary>Without a query the same source is inspected normally — the refusal is about the flag, not the reader.</summary>
	[Fact]
	public async Task A_Reader_That_Takes_No_Query_Is_Inspected_Normally_Without_One()
	{
		var csv = Path.Combine(Directory.GetCurrentDirectory(), $"dtpipe_q_{Guid.NewGuid():N}.csv");
		try
		{
			await File.WriteAllTextAsync(csv, "id,label\n1,a\n");

			var json = await BuildTools().Inspect($"csv:{csv}");

			json.Should().Contain("label");
			json.Should().NotContain("takes no query");
		}
		finally
		{
			if (File.Exists(csv)) File.Delete(csv);
		}
	}
}
