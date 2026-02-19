using System.Diagnostics;
using DtPipe.Adapters.Csv;
using DtPipe.Adapters.DuckDB;
using DtPipe.Cli.Infrastructure;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Transformers.Fake;
using DtPipe.Transformers.Format;
using DtPipe.Transformers.Null;
using DtPipe.Transformers.Overwrite;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;


namespace DtPipe.Tests;

public class PipelineStressTests : IAsyncLifetime
{
	private readonly ITestOutputHelper _output;
	private readonly string _dbPath;
	private readonly string _connectionString;
	private readonly string _outputPath;

	public PipelineStressTests(ITestOutputHelper output)
	{
		_output = output;
		_dbPath = Path.Combine(Path.GetTempPath(), $"stress_{Guid.NewGuid()}.duckdb");
		_connectionString = $"Data Source={_dbPath}";
		_outputPath = Path.Combine(Path.GetTempPath(), $"stress_output_{Guid.NewGuid()}.csv");
	}

	public ValueTask DisposeAsync()
	{
		try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
		try { if (File.Exists(_outputPath)) File.Delete(_outputPath); } catch { }
		return ValueTask.CompletedTask;
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	[Fact]
	[Trait("Category", "Stress")] // Mark as stress test
	public async Task Export_20M_Rows_With_Complex_Ordered_Pipeline()
	{
		// 1. Generate Data (20M rows)
		_output.WriteLine($"Generating 20M rows in {_dbPath}...");
		var sw = Stopwatch.StartNew();

		using (var connection = new DuckDB.NET.Data.DuckDBConnection(_connectionString))
		{
			await connection.OpenAsync(TestContext.Current.CancellationToken);
			using var cmd = connection.CreateCommand();
			// detailed generation: Id, Name (placeholder), Category
			cmd.CommandText = "CREATE TABLE large_data AS SELECT range AS Id, 'PlaceHolder' AS Name, 'Original' AS Category, 'ToBeNulled' AS Temp FROM range(1000000);"; ;
			await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
		}
		_output.WriteLine($"Data generation took {sw.ElapsedMilliseconds}ms");

		// 2. Setup Services
		var registry = new OptionsRegistry();
		registry.Register(new DuckDbReaderOptions());
		registry.Register(new CsvWriterOptions { Header = true });
		// Register transformer options to be populated by builder
		registry.Register(new NullOptions());
		registry.Register(new OverwriteOptions());
		registry.Register(new FormatOptions());
		// Register FakeOptions with specific configuration (Seed/Locale)
		// Global options (Seed/Locale) are still used by Factory.CreateFromConfiguration
		registry.Register(new FakeOptions
		{
			Seed = 12345,
			Locale = "en"
		});

		var services = new ServiceCollection();
		services.AddSingleton(registry);
		services.AddSingleton<IStreamReaderFactory>(sp => new CliStreamReaderFactory(
			new DuckDbReaderDescriptor(),
			sp.GetRequiredService<OptionsRegistry>(),
			sp));
		services.AddSingleton<IDataWriterFactory>(sp => new CliDataWriterFactory(
			new CsvWriterDescriptor(),
			sp.GetRequiredService<OptionsRegistry>(),
			sp));
		services.AddSingleton<IDataTransformerFactory, NullDataTransformerFactory>();
		services.AddSingleton<IDataTransformerFactory, OverwriteDataTransformerFactory>();
		services.AddSingleton<IDataTransformerFactory, FakeDataTransformerFactory>();
		services.AddSingleton<IDataTransformerFactory, FormatDataTransformerFactory>();
		services.AddSingleton<ExportService>();

		var mockProgress = new Mock<IExportProgress>();
		var mockObserver = new Mock<IExportObserver>();
		mockObserver.Setup(o => o.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<IEnumerable<string>>()))
					.Returns(mockProgress.Object);
		services.AddSingleton(mockObserver.Object);

		services.AddLogging(); // Required for Serilog/Loggers used in ExportService
		var serviceProvider = services.BuildServiceProvider();
		var exportService = serviceProvider.GetRequiredService<ExportService>();

		var options = new DumpOptions
		{
			Provider = "duckdb",
			ConnectionString = _connectionString,
			// Updated Query: SELECT Id, Name, Category, Temp, '' as Greeting, '' as Ref, '' as FinalStatus FROM large_data
			Query = "SELECT Id, Name, Category, Temp, '' as Greeting, '' as Ref, '' as FinalStatus FROM large_data",
			OutputPath = _outputPath,
			BatchSize = 10000 // Large batch for performance
		};

		// 3. Define Pipeline Args
		var args = new[]
		{
			"dtpipe",
			"--fake", "Name:name.firstName",
			"--overwrite", "Category:Processed",
			"--format", "Greeting:Hello {Name}",
			"--format", "Ref:Ref_{Id}",
			"--null", "Temp",
			"--format", "FinalStatus:{Category}_{Id}"
		};

		// 4. Run Export
		_output.WriteLine("Starting Export...");
		sw.Restart();

		var transformerFactories = serviceProvider.GetServices<IDataTransformerFactory>().ToList();
		var pipelineBuilder = new TransformerPipelineBuilder(transformerFactories);
		var pipeline = pipelineBuilder.Build(args);
		var readerFactory = serviceProvider.GetRequiredService<IStreamReaderFactory>();
		var writerFactory = serviceProvider.GetRequiredService<IDataWriterFactory>();
		await exportService.RunExportAsync(new PipelineOptions { BatchSize = options.BatchSize }, options.Provider, options.OutputPath, TestContext.Current.CancellationToken, pipeline, readerFactory, writerFactory, registry);

		sw.Stop();
		_output.WriteLine($"Export took {sw.ElapsedMilliseconds}ms ({sw.Elapsed.TotalSeconds:N2}s)");

		var throughput = 1_000_000.0 / sw.Elapsed.TotalSeconds;
		_output.WriteLine($"Throughput: {throughput:N0} rows/s");

		// 5. Verify Output (light check)
		File.Exists(_outputPath).Should().BeTrue();


		// Check first few lines
		using var reader = new StreamReader(_outputPath);
		var header = await reader.ReadLineAsync(TestContext.Current.CancellationToken);
		var line1 = await reader.ReadLineAsync(TestContext.Current.CancellationToken);

		// Validation
		// Header: Id,Name,Category,Temp,Greeting,Ref,FinalStatus
		// Line 1 (Id=0):
		// Name: [Faked] (Deterministic 12345, Name 0 -> ?)
		// Category: Processed
		// Temp: (Empty)
		// Greeting: Hello [Faked]
		// Ref: Ref_0
		// FinalStatus: Processed_0

		var cols = line1!.Split(',');
		// Id is index 0
		cols[0].Should().Be("0");
		// Name index 1
		cols[1].Should().NotBe("PlaceHolder");
		// Category index 2
		cols[2].Should().Be("Processed");
		// Temp index 3
		cols[3].Should().BeEmpty();
		// Greeting index 4
		cols[4].Should().StartWith("Hello ");
		// Ref index 5
		cols[5].Should().Be("Ref_0");
		// FinalStatus index 6
		cols[6].Should().Be("Processed_0");

		// Performance assertion (sanity check, e.g. > 10k rows/s)
		throughput.Should().BeGreaterThan(10_000);
	}
}
