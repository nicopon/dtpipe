using System.CommandLine;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Cli;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Transformers.Fake;
using QueryDump.Writers;

namespace QueryDump;

class Program
{
    static async Task<int> Main(string[] args)
    {
        var services = new ServiceCollection();
        ConfigureServices(services);
        var serviceProvider = services.BuildServiceProvider();

        var rootCommand = CliBuilder.Build(serviceProvider);
        return await rootCommand.InvokeAsync(args);
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        services.AddSingleton<IDataSourceFactory, DataSourceFactory>();
        services.AddSingleton<IDataWriterFactory, DataWriterFactory>();
        services.AddSingleton<IFakeDataTransformerFactory, FakeDataTransformerFactory>();
        services.AddSingleton<OptionsRegistry>();
        services.AddSingleton<ExportService>();
    }
}
