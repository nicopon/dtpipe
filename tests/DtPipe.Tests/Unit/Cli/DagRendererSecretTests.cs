using System;
using System.IO;
using System.Linq;
using DtPipe.Cli;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using Spectre.Console;
using Spectre.Console.Rendering;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A 'keyring://' reference reaches the banner already expanded — JobFileParser resolves it before
/// the renderer runs — so a job file naming one printed the password to the terminal that the
/// keyring exists to keep off it.
/// </summary>
public class DagRendererSecretTests
{
    private const string Secret = "s3cr3t-p4ssw0rd";
    private const string Resolved = "mssql:Server=db.example.net;User ID=app;Password=" + Secret + ";Encrypt=True";

    private static string Render(IRenderable renderable)
    {
        var writer = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Out = new AnsiConsoleOutput(writer)
        });
        console.Profile.Width = 400;
        console.Write(renderable);
        return writer.ToString();
    }

    [Fact]
    public void LinearPanel_MasksThePasswordInTheInput()
    {
        var text = Render(DagRenderer.BuildLinearTopologyPanel(
            new JobDefinition { Input = Resolved, Output = "out.csv" },
            Array.Empty<IStreamReaderFactory>()));

        Assert.DoesNotContain(Secret, text);
        Assert.Contains("Password=***", text);
        Assert.Contains("out.csv", text);
    }

    [Fact]
    public void LinearPanel_MasksThePasswordInTheOutput()
    {
        var text = Render(DagRenderer.BuildLinearTopologyPanel(
            new JobDefinition { Input = "in.csv", Output = Resolved },
            Array.Empty<IStreamReaderFactory>()));

        Assert.DoesNotContain(Secret, text);
        Assert.Contains("Password=***", text);
    }

    [Fact]
    public void TopologyPanel_MasksThePasswordOnEveryBranchRole()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "src", Input = Resolved },
                new BranchDefinition { Alias = "sink", StreamingAliases = new[] { "src" }, Output = Resolved },
                new BranchDefinition
                {
                    Alias = "join",
                    ProcessorName = "sql",
                    Arguments = new[] { "--sql", "SELECT 1" },
                    StreamingAliases = new[] { "src" },
                    Output = Resolved
                }
            }
        };

        var text = Render(DagRenderer.BuildTopologyPanel(dag, Array.Empty<IStreamReaderFactory>()));

        Assert.DoesNotContain(Secret, text);
        Assert.Equal(3, text.Split("Password=***").Length - 1);
    }

    /// <summary>A keyring reference names no secret, so it stays readable.</summary>
    [Fact]
    public void AnUnresolvedKeyringReference_IsShownAsIs()
    {
        var text = Render(DagRenderer.BuildLinearTopologyPanel(
            new JobDefinition { Input = "keyring://VOLTS", Output = "out.csv" },
            Array.Empty<IStreamReaderFactory>()));

        Assert.Contains("keyring://VOLTS", text);
    }
}
