using System.Text.RegularExpressions;
using DtPipe.Cli;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>The file path of a jsonl or xml source or sink is given by the selector; no bare flag carries it.</summary>
[Collection("console-serial")]
public sealed class FileAdapterPathFlagsHelpTests
{
    [Fact]
    public void The_Help_Declares_No_Bare_Jsonl_Or_Xml_Flag_Yet_Keeps_Their_Sibling_Options()
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

        var help = captured.ToString();

        Assert.DoesNotMatch(new Regex(@"^\s*--(jsonl|xml)(\s|$)", RegexOptions.Multiline), help);
        Assert.Contains("--jsonl-encoding", help, StringComparison.Ordinal);
        Assert.Contains("--xml-namespaces", help, StringComparison.Ordinal);
    }
}
