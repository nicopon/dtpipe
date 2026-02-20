using DtPipe.Adapters.DuckDB;
using DtPipe.Adapters.Parquet;
using DtPipe.Cli.Infrastructure;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Transformers.Fake;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace DtPipe.Tests;

public class ManualVerificationTests : IAsyncLifetime
{
	private readonly string _dbPath;
	private readonly string _connectionString;
	private readonly string _outputPath;
	private readonly ITestOutputHelper _output;

	public ManualVerificationTests(ITestOutputHelper output)
	{
		_output = output;
		_dbPath = Path.Combine(Path.GetTempPath(), $"dryrun_verify_{Guid.NewGuid()}.duckdb");
		_connectionString = $"Data Source={_dbPath}";
		_outputPath = Path.Combine(Path.GetTempPath(), $"dryrun_verify_{Guid.NewGuid()}.parquet");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
		try { if (File.Exists(_outputPath)) File.Delete(_outputPath); } catch { }
		GC.SuppressFinalize(this);
		return ValueTask.CompletedTask;
	}

	[Fact]
	[Trait("Category", "ManualVerification")]
	public async Task Verify_DuckDB_To_Parquet_With_Fake_Transform()
	{
		// 1. Setup Database with Sensitive Data
		using (var connection = new DuckDB.NET.Data.DuckDBConnection(_connectionString))
		{
			await connection.OpenAsync(TestContext.Current.CancellationToken);
			using var cmd = connection.CreateCommand();
			cmd.CommandText = "CREATE TABLE users (Id INTEGER, Name VARCHAR, Email VARCHAR)";
			await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

			cmd.CommandText = "INSERT INTO users VALUES (1, 'Sensitive Name', 'sensitive@example.com')";
			await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
		}

		// Verify DB file exists
		if (File.Exists(_dbPath))
		{
			_output.WriteLine($"DB File created: {_dbPath} ({new FileInfo(_dbPath).Length} bytes)");
		}
		else
		{
			_output.WriteLine($"DB File NOT found: {_dbPath}");
		}

		// 2. Configure Services
		var registry = new OptionsRegistry();
		registry.Register(new DuckDbReaderOptions { Query = "SELECT * FROM users" });
		// Faking rule: Name -> name.fullName
		registry.Register(new FakeOptions
		{
			Fake = ["Name:name.fullName"],
			Seed = 12345 // Deterministic
		});

		var services = new ServiceCollection();
		services.AddLogging();
		services.AddSingleton(registry);

		services.AddSingleton<IStreamReaderFactory>(sp => new CliStreamReaderFactory(
			new DuckDbReaderDescriptor(),
			sp.GetRequiredService<OptionsRegistry>(),
			sp));
		services.AddSingleton<IDataWriterFactory>(sp => new CliDataWriterFactory(
			new ParquetWriterDescriptor(),
			sp.GetRequiredService<OptionsRegistry>(),
			sp));
		services.AddSingleton<IDataTransformerFactory, FakeDataTransformerFactory>();

		services.AddSingleton<ExportService>();

		var mockProgress = new Mock<IExportProgress>();
		var mockObserver = new Mock<IExportObserver>();
		mockObserver.Setup(o => o.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<IEnumerable<string>>()))
					.Returns(mockProgress.Object);
		services.AddSingleton(mockObserver.Object);

		var serviceProvider = services.BuildServiceProvider();
		var exportService = serviceProvider.GetRequiredService<ExportService>();

		// 3. Define Run Options
		var options = new DumpOptions
		{
			Provider = "duckdb",
			ConnectionString = _connectionString,
			Query = "SELECT * FROM users",
			OutputPath = _outputPath,
			BatchSize = 100
		};
		registry.Register(options);

		// 4. Build Pipeline
		var transformerFactories = serviceProvider.GetServices<IDataTransformerFactory>().ToList();
		var pipelineBuilder = new TransformerPipelineBuilder(transformerFactories);
		var args = new[] { "dtpipe", "--fake", "Name:name.fullName" };
		var pipeline = pipelineBuilder.Build(args);

		// 5. Run Export
		try
		{
			var readerFactory = serviceProvider.GetRequiredService<IStreamReaderFactory>();
			var writerFactory = serviceProvider.GetRequiredService<IDataWriterFactory>();
			await exportService.RunExportAsync(new PipelineOptions { BatchSize = options.BatchSize }, options.Provider, options.OutputPath, TestContext.Current.CancellationToken, pipeline, readerFactory, writerFactory, registry);
		}
		catch (Exception ex)
		{
			_output.WriteLine($"Export Failed: {ex}");
			throw;
		}

		// 6. Verify Output
		if (File.Exists(_outputPath))
		{
			var info = new FileInfo(_outputPath);
			_output.WriteLine($"Parquet file created. Size: {info.Length} bytes");
		}
		else
		{
			_output.WriteLine("Parquet file NOT created.");
		}

		File.Exists(_outputPath).Should().BeTrue();

		// Read back Parquet using Parquet.Net to verify validity
		using (var stream = File.OpenRead(_outputPath))
		{
			using (var reader = await Parquet.ParquetReader.CreateAsync(stream))
			{
				reader.RowGroupCount.Should().BeGreaterThan(0);
				var group = reader.OpenRowGroupReader(0);
				var idCol = await group.ReadColumnAsync(reader.Schema.GetDataFields()[0]);
				var nameCol = await group.ReadColumnAsync(reader.Schema.GetDataFields()[1]);
				var emailCol = await group.ReadColumnAsync(reader.Schema.GetDataFields()[2]);

				idCol.Data.GetValue(0).Should().Be(1);
				nameCol.Data.GetValue(0).Should().NotBe("Sensitive Name");
				emailCol.Data.GetValue(0).Should().Be("sensitive@example.com");
			}
		}
	}
}
