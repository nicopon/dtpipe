using System.Runtime.CompilerServices;
using DtPipe.Cli;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Transformers.Services;
using DtPipe.Observers;
using DtPipe.Transformers.Arrow.Fake;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Arrow.Format;
using DtPipe.Transformers.Arrow.Mask;
using DtPipe.Transformers.Arrow.Null;
using DtPipe.Transformers.Arrow.Overwrite;
using DtPipe.Transformers.Arrow.Project;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Row.Window;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;

[assembly: InternalsVisibleTo("DtPipe.Tests")]
namespace DtPipe;

class Program
{
	internal static async Task<int> Main(string[] args)
	{
		System.Globalization.CultureInfo.DefaultThreadCurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
		System.Globalization.CultureInfo.DefaultThreadCurrentUICulture = System.Globalization.CultureInfo.InvariantCulture;
		Console.OutputEncoding = System.Text.Encoding.UTF8;

		var services = new ServiceCollection();
		ConfigureServices(services);
		var serviceProvider = services.BuildServiceProvider();

		Log.Logger = new LoggerConfiguration()
			.MinimumLevel.Debug()
			.WriteTo.Console(
				outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}",
				standardErrorFromLevel: Serilog.Events.LogEventLevel.Verbose)
			.CreateLogger();

		var jobService = serviceProvider.GetRequiredService<JobService>();

		// Ensure cursor is restored on Ctrl+C
		Console.CancelKeyPress += (_, _) => Console.CursorVisible = true;

		try
		{
            // 1. Detect Subcommand vs Pipeline
            if (args.Length > 0 && IsSubcommand(args[0]))
            {
                var rootCommand = jobService.BuildSubcommands();
                return await rootCommand.Parse(args).InvokeAsync();
            }

            // 2. Help Interception
            if (args.Length == 0 || args.Any(a => a == "--help" || a == "-h" || a == "-?"))
            {
                var (_, printHelp, _, _, _) = jobService.Build();
                printHelp();
                return 0;
            }

            // 3. Pipeline Execution (New Parser)
            var registry = new FlagRegistry();
            CoreFlagRegistry.RegisterCoreFlags(registry);
            var contributors = new List<ICliContributor>();
            contributors.AddRange(serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>().OfType<ICliContributor>());
            contributors.AddRange(serviceProvider.GetRequiredService<IEnumerable<IDataTransformerFactory>>().OfType<ICliContributor>());
            contributors.AddRange(serviceProvider.GetRequiredService<IEnumerable<IDataWriterFactory>>().OfType<ICliContributor>());

            foreach (var contributor in contributors)
            {
                foreach (var def in contributor.GetFlagDefs())
                {
                    registry.Register(def);
                    if (Environment.GetEnvironmentVariable("DEBUG") == "1") Console.Error.WriteLine($"[DEBUG] Registered flag: {def.Name} (Arity: {def.Arity})");
                }
            }

            // Register CLI trigger flags from stream processor factories
            var streamTransformerFactories = serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();
            foreach (var stf in streamTransformerFactories)
            {
                foreach (var (flag, isBoolean) in stf.CliTriggerFlags)
                {
                    var arity = isBoolean ? FlagArity.Boolean : FlagArity.Scalar;
                    registry.Register(new FlagDef(flag, Array.Empty<string>(), arity, FlagScope.PerBranch, stf.ComponentName));
                    if (Environment.GetEnvironmentVariable("DEBUG") == "1") Console.Error.WriteLine($"[DEBUG] Registered processor flag: {flag} (Arity: {arity}, Processor: {stf.ComponentName})");
                }
            }

            var lexer = new PipelineLexer(registry);
            var parsedPipeline = lexer.Parse(args);
            var (jobs, dag) = PipelineToJobConverter.Convert(parsedPipeline, streamTransformerFactories);

            return await jobService.ExecutePipelineAsync(jobs, dag, parsedPipeline.Globals, CancellationToken.None);
		}
		catch (Exception ex)
		{
            Console.Error.WriteLine($"[ERROR] {ex.Message}");
            if (Environment.GetEnvironmentVariable("DEBUG") == "1") Console.Error.WriteLine(ex.StackTrace);
			return 1;
		}
		finally
		{
			try { Console.CursorVisible = true; } catch { }
			await Log.CloseAndFlushAsync();
		}
	}

    private static bool IsSubcommand(string arg)
    {
        var subs = new[] { "inspect", "providers", "completion", "secret" };
        return subs.Contains(arg.ToLowerInvariant());
    }

	private static void ConfigureServices(IServiceCollection services)
	{
		services.AddLogging(logging => {
			logging.ClearProviders();
			logging.AddSerilog(Log.Logger);
			logging.SetMinimumLevel(LogLevel.Debug);
		});

		services.AddSingleton<OptionsRegistry>();
		services.AddSingleton<Spectre.Console.IAnsiConsole>(sp => Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings { Out = new Spectre.Console.AnsiConsoleOutput(Console.Error) }));
		services.AddSingleton<JobService>();

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

		RegisterStreamTransformer<DtPipe.Processors.Sql.CompositeSqlTransformerFactory>(services);
		RegisterStreamTransformer<DtPipe.Processors.Merge.MergeTransformerFactory>(services);

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

		services.AddSingleton<IJsEngineProvider, JsEngineProvider>();
		services.AddSingleton<IExportObserver, SpectreConsoleObserver>();
		services.AddSingleton<DtPipe.Services.HookExecutor>();
		services.AddSingleton<DtPipe.Services.MetricsService>();
		services.AddSingleton<DtPipe.Services.SchemaValidationService>();
		services.AddSingleton<DtPipe.Services.PipelineExecutor>();
		services.AddSingleton<ExportService>();
		services.AddSingleton<IMemoryChannelRegistry, MemoryChannelRegistry>();
		services.AddTransient<IDagOrchestrator, DagOrchestrator>();
		services.AddSingleton<IRowToColumnarBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory>();
		services.AddSingleton<IColumnarToRowBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory>();
	}

	private static void RegisterStreamTransformer<TFac>(IServiceCollection services) where TFac : class, IStreamTransformerFactory => services.AddSingleton<IStreamTransformerFactory>(sp => ActivatorUtilities.CreateInstance<TFac>(sp));
	private static void RegisterWriter<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IDataWriter>, new()
	{
		var descriptor = new TDesc();
		services.AddSingleton<IDataWriterFactory>(sp => new CliDataWriterFactory(descriptor, sp.GetRequiredService<OptionsRegistry>(), sp));
		services.AddSingleton<ICliContributor>(sp => (ICliContributor)sp.GetServices<IDataWriterFactory>().First(f => f.ComponentName == descriptor.ComponentName));
	}
	private static void RegisterReader<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IStreamReader>, new()
	{
		var descriptor = new TDesc();
		services.AddSingleton<IStreamReaderFactory>(sp => new CliStreamReaderFactory(descriptor, sp.GetRequiredService<OptionsRegistry>(), sp));
		services.AddSingleton<ICliContributor>(sp => (ICliContributor)sp.GetServices<IStreamReaderFactory>().First(f => f.ComponentName == descriptor.ComponentName));
	}
	private static void RegisterTransformer<TFac>(IServiceCollection services) where TFac : class, IDataTransformerFactory 
    {
        services.AddSingleton<IDataTransformerFactory>(sp => new CliDataTransformerFactory(ActivatorUtilities.CreateInstance<TFac>(sp)));
    }
}
