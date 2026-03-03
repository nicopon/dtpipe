using DtPipe.Cli;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.XStreamers.DuckDB;
using DtPipe.Transformers.Services;
using DtPipe.Observers;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Columnar.Fake;
using DtPipe.Transformers.Columnar.Filter;
using DtPipe.Transformers.Columnar.Format;
using DtPipe.Transformers.Columnar.Mask;
using DtPipe.Transformers.Columnar.Null;
using DtPipe.Transformers.Columnar.Overwrite;
using DtPipe.Transformers.Columnar.Project;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Window;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;

namespace DtPipe;

class Program
{
	static async Task<int> Main(string[] args)
	{
		System.Globalization.CultureInfo.DefaultThreadCurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
		System.Globalization.CultureInfo.DefaultThreadCurrentUICulture = System.Globalization.CultureInfo.InvariantCulture;
		Console.OutputEncoding = System.Text.Encoding.UTF8;

		var services = new ServiceCollection();
		ConfigureServices(services);
		var serviceProvider = services.BuildServiceProvider();

		// Initialize Serilog with default console logger
		Log.Logger = new LoggerConfiguration()
			.MinimumLevel.Debug()
			.WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
			.CreateLogger();

		var jobService = serviceProvider.GetRequiredService<JobService>();
		var (rootCommand, printHelp) = jobService.Build();

		if (args.Length == 0 || args.Any(a => a == "--help" || a == "-h" || a == "-?"))
		{
			printHelp();
			return 0;
		}

		// Ensure cursor is restored on Ctrl+C
		Console.CancelKeyPress += (_, _) => Console.CursorVisible = true;

		try
		{
			var effectiveArgs = args;

			return await rootCommand.Parse(effectiveArgs).InvokeAsync();
		}
		finally
		{
			// Ensure cursor is always visible upon exit, even after crash or Ctrl+C
			try { Console.CursorVisible = true; } catch { /* Ignore if unable to access console */ }
			await Log.CloseAndFlushAsync();
		}
	}

	private static void ConfigureServices(IServiceCollection services)
	{
		// Serilog
		services.AddLogging(logging =>
		{
			logging.ClearProviders();
			logging.AddSerilog(Log.Logger);
			logging.SetMinimumLevel(LogLevel.Debug);
		});

		// Configuration
		services.AddSingleton<OptionsRegistry>();
		services.AddSingleton<Spectre.Console.IAnsiConsole>(sp =>
		{
			return Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings
			{
				Out = new Spectre.Console.AnsiConsoleOutput(Console.Error)
			});
		});

		// CLI
		services.AddSingleton<JobService>();

		// Provider Auto-Discovery
		// Explicitly Register Readers (Adapters)
		RegisterReader<DtPipe.Adapters.Arrow.ArrowReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Csv.CsvReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.DuckDB.DuckDataSourceReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.DuckDB.DuckDbReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Generate.GenerateReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.JsonL.JsonLReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.MemoryChannel.MemoryChannelReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Oracle.OracleReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Parquet.ParquetReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.PostgreSQL.PostgreSqlReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Sqlite.SqliteReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.SqlServer.SqlServerReaderDescriptor>(services);

		// Explicitly Register Writers (Adapters)
		RegisterWriter<DtPipe.Adapters.Arrow.ArrowWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Checksum.ChecksumWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Csv.CsvWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.DuckDB.DuckDbWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.JsonL.JsonLWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.MemoryChannel.ArrowMemoryChannelWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.MemoryChannel.MemoryChannelWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Null.NullDataWriterFactory>(services);
		RegisterWriter<DtPipe.Adapters.Oracle.OracleWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Parquet.ParquetWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.PostgreSQL.PostgreSqlWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Sqlite.SqliteWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.SqlServer.SqlServerWriterDescriptor>(services);

		// Explicitly Register XStreamers
		RegisterXStreamer<DtPipe.XStreamers.DuckDB.DuckDBXStreamerFactory>(services);
		RegisterXStreamer<DtPipe.XStreamers.DataFusion.DataFusionXStreamerFactory>(services);

		// Transformer Factories
		RegisterTransformer<NullDataTransformerFactory>(services);
		RegisterTransformer<OverwriteDataTransformerFactory>(services);
		RegisterTransformer<FakeDataTransformerFactory>(services);
		RegisterTransformer<FormatDataTransformerFactory>(services);
		RegisterTransformer<MaskDataTransformerFactory>(services);
		RegisterTransformer<ComputeDataTransformerFactory>(services);
		RegisterTransformer<FilterDataTransformerFactory>(services);
		RegisterTransformer<ExpandDataTransformerFactory>(services);
		RegisterTransformer<ProjectDataTransformerFactory>(services);
		RegisterTransformer<WindowDataTransformerFactory>(services);

		// Services
		services.AddSingleton<IJsEngineProvider, JsEngineProvider>();

		// Export Service
		services.AddSingleton<IExportObserver, SpectreConsoleObserver>();
		services.AddSingleton<ExportService>();

		// DAG Orchestrator & Memory Channels
		services.AddSingleton<IMemoryChannelRegistry, MemoryChannelRegistry>();
		services.AddTransient<IDagOrchestrator, DagOrchestrator>();

		// XStreamers are now auto-discovered as readers, no custom factory needed

		// Columnar Bridges
		services.AddSingleton<IRowToColumnarBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory>();
		services.AddSingleton<IColumnarToRowBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory>();
	}

	private static void RegisterXStreamer<TDesc>(IServiceCollection services) where TDesc : class, IXStreamerFactory, new()
	{
		services.AddSingleton<IXStreamerFactory>(sp => new CliXStreamerFactory(
			new TDesc(),
			sp.GetRequiredService<OptionsRegistry>(),
			sp
		));
	}

	private static void RegisterWriter<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IDataWriter>, new()
	{
		services.AddSingleton<IDataWriterFactory>(sp => new CliDataWriterFactory(
			new TDesc(),
			sp.GetRequiredService<OptionsRegistry>(),
			sp
		));
	}

	private static void RegisterReader<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IStreamReader>, new()
	{
		services.AddSingleton<IStreamReaderFactory>(sp => new CliStreamReaderFactory(
			new TDesc(),
			sp.GetRequiredService<OptionsRegistry>(),
			sp
		));
	}

	private static void RegisterTransformer<TFac>(IServiceCollection services) where TFac : class, IDataTransformerFactory
	{
		services.AddSingleton<IDataTransformerFactory>(sp =>
		{
			var inner = ActivatorUtilities.CreateInstance<TFac>(sp);
			return new CliDataTransformerFactory(inner);
		});
	}
}
