using DtPipe.Cli;
using DtPipe.Cli.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>A bare '-' names no format, so no source resolves it: the help names the prefixed form instead.</summary>
[Collection("console-serial")]
public sealed class StdinStdoutHelpTextTests
{
    [Fact]
    public void The_Help_Names_The_Prefixed_Form_For_Stdin_And_Stdout_Never_A_Bare_Dash()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        using var provider = services.BuildServiceProvider();

        var error = Console.Error;
        var captured = new StringWriter();
        Console.SetError(captured);
        try
        {
            HelpRenderer.Print(provider);
        }
        finally
        {
            Console.SetError(error);
        }

        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        var texts = new[]
        {
            HelpLine(captured.ToString(), "--input, -i"),
            HelpLine(captured.ToString(), "--output, -o"),
            registry.Lookup("--input")!.Description!,
            registry.Lookup("--output")!.Description!,
        };

        foreach (var text in texts)
        {
            Assert.Contains("'<provider>:-'", text, StringComparison.Ordinal);
            Assert.DoesNotContain("or '-'", text, StringComparison.Ordinal);
        }
    }

    private static string HelpLine(string help, string flag) =>
        help.Split('\n').Single(l => l.TrimStart().StartsWith(flag, StringComparison.Ordinal));
}
