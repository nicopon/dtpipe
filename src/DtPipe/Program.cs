using System.Runtime.CompilerServices;
using DtPipe.Cli;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Transformers.Services;
using DtPipe.Observers;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Arrow.Fake;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Arrow.Format;
using DtPipe.Transformers.Arrow.Mask;
using DtPipe.Transformers.Arrow.Null;
using DtPipe.Transformers.Arrow.Overwrite;
using DtPipe.Transformers.Arrow.Project;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Window;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;

[assembly: InternalsVisibleTo("DtPipe.Tests")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]

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

		// Initialize Serilog with default console logger (pointing to STDERR)
		Log.Logger = new LoggerConfiguration()
			.MinimumLevel.Debug()
			.WriteTo.Console(
				outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}",
				standardErrorFromLevel: Serilog.Events.LogEventLevel.Verbose)
			.CreateLogger();

		var jobService = serviceProvider.GetRequiredService<JobService>();
		var (rootCommand, printHelp, coreOptions, flagPhases, contributors) = jobService.Build();

		// Ensure cursor is restored on Ctrl+C
		Console.CancelKeyPress += (_, _) => Console.CursorVisible = true;

		try
		{
			// Pre-process args to escape leading '@' to avoid response file expansion in System.CommandLine
			var effectiveArgs = args.Select(arg => arg.StartsWith("@") ? " " + arg : arg).ToArray();

            // Manual Autocompletion Support ([suggest] directive might be missing in 2.0.3 default Pipeline)
            if (effectiveArgs.Length > 1 && effectiveArgs[0].Equals("[suggest]", StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(effectiveArgs[1], out var rawPos))
                {
                    // The shell scripts pass the command words starting with the executable name.
                    // e.g. [suggest] <pos> dtpipe --input ... (wait, no. The shells pass the arguments AFTER the executable!).
                    // Shell script: dtpipe [suggest] $pos "${args[@]:1}"
                    // Since args[@]:1 strips the executable name, the array is shifted by 1.
                    // But $pos still includes the executable index count!
                    // We must deduct 1 from pos to match our effective array skipping the executable.
                    int pos = Math.Max(0, rawPos - 1);

                    var rawWords = effectiveArgs.Skip(2).ToArray();
                    if (pos >= rawWords.Length)
                    {
                        var newWords = new string[pos + 1];
                        Array.Copy(rawWords, newWords, rawWords.Length);
                        for (int i = rawWords.Length; i < newWords.Length; i++) newWords[i] = "";
                        rawWords = newWords;
                    }

                    var completions = ContextualCompletionProvider.GetCompletions(
                        rootCommand,
                        rawWords,
                        pos,
                        coreOptions.AllOptions,
                        flagPhases,
                        contributors);

                    foreach (var c in completions)
                        Console.WriteLine(c);

                    return 0;
                }
            }

            // Custom Help Interception
            bool isSuggest = effectiveArgs.Length > 0 && effectiveArgs[0] == "[suggest]";
            if (!isSuggest && (effectiveArgs.Length == 0 || effectiveArgs.Any(a => a == "--help" || a == "-h" || a == "-?" || a == "/?" || a == "/h")))
            {
                printHelp();
                return 0;
            }

            var parseResult = rootCommand.Parse(effectiveArgs);
            if (parseResult.Errors.Any())
            {
                foreach (var error in parseResult.Errors)
                {
                    Console.Error.WriteLine(error.Message);
                }
                return 1;
            }
			var result = await parseResult.InvokeAsync();
			return Environment.ExitCode != 0 ? Environment.ExitCode : result;
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
		RegisterReader<DtPipe.Adapters.DuckDB.DuckDbReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Generate.GenerateReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.JsonL.JsonLReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.MemoryChannel.ArrowMemoryChannelReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.MemoryChannel.MemoryChannelReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Oracle.OracleReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Parquet.ParquetReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.PostgreSQL.PostgreSqlReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Sqlite.SqliteReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.SqlServer.SqlServerReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Xml.XmlReaderDescriptor>(services);

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

		// Register Stream Transformers
		RegisterStreamTransformer<DtPipe.Processors.Sql.CompositeSqlTransformerFactory>(services);
		RegisterStreamTransformer<DtPipe.Processors.Merge.MergeTransformerFactory>(services);

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
		services.AddSingleton<DtPipe.Services.HookExecutor>();
		services.AddSingleton<DtPipe.Services.MetricsService>();
		services.AddSingleton<DtPipe.Services.SchemaValidationService>();
		services.AddSingleton<DtPipe.Services.PipelineExecutor>();
		services.AddSingleton<ExportService>();

		// DAG Orchestrator & Memory Channels
		services.AddSingleton<IMemoryChannelRegistry, MemoryChannelRegistry>();
		services.AddTransient<IDagOrchestrator, DagOrchestrator>();

		// Columnar Bridges
		services.AddSingleton<IRowToColumnarBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory>();
		services.AddSingleton<IColumnarToRowBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory>();
	}

	private static void RegisterStreamTransformer<TFac>(IServiceCollection services) where TFac : class, IStreamTransformerFactory
	{
		services.AddSingleton<IStreamTransformerFactory>(sp =>
			ActivatorUtilities.CreateInstance<TFac>(sp));
	}

	private static void RegisterWriter<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IDataWriter>, new()
	{
		var descriptor = new TDesc();
		services.AddSingleton<IDataWriterFactory>(sp => {
			return new CliDataWriterFactory(
				descriptor,
				sp.GetRequiredService<OptionsRegistry>(),
				sp
			);
		});
		services.AddSingleton<ICliContributor>(sp => (ICliContributor)sp.GetServices<IDataWriterFactory>().First(f => f.ComponentName == descriptor.ComponentName));
	}

	private static void RegisterReader<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IStreamReader>, new()
	{
		var descriptor = new TDesc();
		services.AddSingleton<IStreamReaderFactory>(sp => {
			return new CliStreamReaderFactory(
				descriptor,
				sp.GetRequiredService<OptionsRegistry>(),
				sp
			);
		});
		services.AddSingleton<ICliContributor>(sp => (ICliContributor)sp.GetServices<IStreamReaderFactory>().First(f => f.ComponentName == descriptor.ComponentName));
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
