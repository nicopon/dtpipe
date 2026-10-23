using System.Text.Json;
using DtPipe.Cli.Commands;
using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

[Collection("console-serial")]
public class ProvidersCommandTests
{
    private static async Task<(string Stdout, int ExitCode)> RunAsync(IServiceProvider sp, params string[] args)
    {
        var prev = Console.Out;
        var captured = new StringWriter();
        Console.SetOut(captured);
        try
        {
            var exit = await new ProvidersCommand(sp).Parse(args).InvokeAsync();
            return (captured.ToString(), exit);
        }
        finally { Console.SetOut(prev); }
    }

    private static IServiceProvider Services()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Json_Lists_Every_Registered_Component_On_Stdout()
    {
        var sp = Services();

        var (stdout, exit) = await RunAsync(sp, "--format", "json");

        Assert.Equal(0, exit);
        var entries = JsonDocument.Parse(stdout).RootElement.EnumerateArray().ToList();
        var expected = sp.GetServices<IStreamReaderFactory>().Count()
            + sp.GetServices<IDataWriterFactory>().Count()
            + sp.GetServices<IStreamTransformerFactory>().Count();
        Assert.Equal(expected, entries.Count);

        foreach (var reader in sp.GetServices<IStreamReaderFactory>())
            Assert.Contains(entries, e => e.GetProperty("name").GetString() == reader.ComponentName && e.GetProperty("kind").GetString() == "reader");
        foreach (var writer in sp.GetServices<IDataWriterFactory>())
            Assert.Contains(entries, e => e.GetProperty("name").GetString() == writer.ComponentName && e.GetProperty("kind").GetString() == "writer");
        foreach (var processor in sp.GetServices<IStreamTransformerFactory>())
            Assert.Contains(entries, e => e.GetProperty("name").GetString() == processor.ComponentName && e.GetProperty("kind").GetString() == "stream-processor");
    }

    [Fact]
    public async Task The_Table_Leaves_Stdout_Empty()
    {
        var (stdout, exit) = await RunAsync(Services());

        Assert.Equal(0, exit);
        Assert.Equal("", stdout);
    }
}
