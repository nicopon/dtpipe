using System.CommandLine;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Cli;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Transformers.Fake;
using QueryDump.Writers;

namespace QueryDump;

class Program
{
    static Task<int> Main(string[] args)
    {
        var services = new ServiceCollection();
        ConfigureServices(services);
        var serviceProvider = services.BuildServiceProvider();

        var rootCommand = CliBuilder.Build(serviceProvider);
        return rootCommand.Parse(args).InvokeAsync();
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        // Discover option types from factories
        services.AddSingleton<IOptionTypeProvider, OptionTypeProvider>();
        
        // Core services
        services.AddSingleton<IDataSourceFactory, DataSourceFactory>();
        services.AddSingleton<IDataWriterFactory, DataWriterFactory>();
        services.AddSingleton<IFakeDataTransformerFactory, FakeDataTransformerFactory>();
        services.AddSingleton<OptionsRegistry>();
        services.AddSingleton<ExportService>();
    }
}
