using System.IO;
using System.Threading.Tasks;
using DtPipe.Cli;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

[Collection("console-serial")]
public class InspectCommandTests
{
    [Fact]
    public async Task The_Header_Shows_The_Source_Without_Its_Password()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);

        var text = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(text),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        console.Profile.Width = 200;
        services.AddSingleton<IAnsiConsole>(console);   // last registration wins

        var jobService = services.BuildServiceProvider().GetRequiredService<JobService>();
        var exit = await jobService.BuildSubcommands()
            .Parse(["inspect", "-i", "duck:Data Source=:memory:;Password=not-a-real-secret", "-q", "select 1 as a"])
            .InvokeAsync();

        Assert.Equal(0, exit);
        Assert.DoesNotContain("not-a-real-secret", text.ToString());
        Assert.Contains("Schema for duck:Data Source=:memory:;Password=***", text.ToString());
    }
}
