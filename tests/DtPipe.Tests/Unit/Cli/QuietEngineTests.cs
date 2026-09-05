using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E2b: an engine run started for a program, not a person, must emit no
/// human-facing rendering. The MCP tools hand their result back as JSON; the DAG topology panel,
/// the results table and the orchestrator log events are decoration nobody reads — and, under the
/// agent's full-screen surface, decoration that lands on a screen the toolkit owns.
/// </summary>
[Collection("console-serial")]
public class QuietEngineTests
{
    private const string Yaml = "main:\n  input: \"generate:5\"\n  output: \"null:\"\n";

    private static (ServiceProvider Sp, StringWriter Out) BuildEngine()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);

        var sw = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(sw),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        console.Profile.Width = 120;
        console.Profile.Height = 40;
        services.AddSingleton<IAnsiConsole>(console);   // last registration wins

        return (services.BuildServiceProvider(), sw);
    }

    private static (Dictionary<string, JobDefinition> Jobs, JobDagDefinition Dag) Parse(DtPipeMcpTools tools, string yaml)
    {
        var method = typeof(DtPipeMcpTools).GetMethod("ParseAndValidateYaml", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var result = method.Invoke(tools, new object[] { yaml })!;
        var jobs = (Dictionary<string, JobDefinition>)result.GetType().GetProperty("Jobs")!.GetValue(result)!;
        var dag = (JobDagDefinition)result.GetType().GetProperty("Dag")!.GetValue(result)!;
        return (jobs, dag);
    }

    private static Dictionary<string, CliJobContext> Contexts(Dictionary<string, JobDefinition> jobs)
        => jobs.ToDictionary(kv => kv.Key, _ => new CliJobContext(null, null, null, Array.Empty<string>()));

    [Fact]
    public async Task Quiet_False_Renders_The_Topology()
    {
        var (sp, sw) = BuildEngine();
        var tools = sp.GetRequiredService<DtPipeMcpTools>();
        var (jobs, dag) = Parse(tools, Yaml);
        var jobService = sp.GetRequiredService<JobService>();

        var exit = await jobService.ExecutePipelineAsync(jobs, dag, Contexts(jobs), new GlobalOptions(), CancellationToken.None);

        Assert.Equal(0, exit);
        Assert.NotEqual(string.Empty, sw.ToString().Trim());
        Assert.Contains("generate", sw.ToString());
    }

    [Fact]
    public async Task Quiet_True_Writes_Nothing()
    {
        var (sp, sw) = BuildEngine();
        var tools = sp.GetRequiredService<DtPipeMcpTools>();
        var (jobs, dag) = Parse(tools, Yaml);
        var jobService = sp.GetRequiredService<JobService>();

        var exit = await jobService.ExecutePipelineAsync(jobs, dag, Contexts(jobs), new GlobalOptions { Quiet = true }, CancellationToken.None);

        Assert.Equal(0, exit);
        Assert.Equal(string.Empty, sw.ToString());
    }

    [Fact]
    public async Task The_Mcp_Dry_Run_Path_Emits_Nothing_To_The_Console()
    {
        var (sp, sw) = BuildEngine();
        var tools = sp.GetRequiredService<DtPipeMcpTools>();

        // apply=false => a real sample run through JobService (DtPipeMcpTools passes Quiet = true).
        var json = await tools.ExecuteYamlJob(Yaml);

        Assert.Contains("\"mode\": \"sample\"", json);
        Assert.Equal(string.Empty, sw.ToString());
    }
}
