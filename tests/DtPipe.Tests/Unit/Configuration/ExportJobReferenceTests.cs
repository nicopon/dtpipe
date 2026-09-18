using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Options;
using Xunit;
using System;
using System.IO;

namespace DtPipe.Tests.Unit.Configuration;

/// <summary>
/// What enters as a reference leaves as a reference — through a job file as well as through a
/// command line.
/// </summary>
/// <remarks>
/// The two entry points disagreed. From a command line nothing interpolates before the writer, so
/// <c>${{MY_DB}}</c> survived into the exported YAML; through <c>--job</c>, every scalar was
/// resolved as it was read, so the value the reference existed to hide was written out in clear,
/// into a -rw-r--r-- file bound for a repository. A keyring alias that did NOT resolve survived,
/// which is what made the leak read as absent.
/// </remarks>
public class ExportJobReferenceTests : IDisposable
{
    private const string Variable = "DTPIPE_TEST_EXPORT_REFERENCE";
    private const string Secret = "S3cr3t!Pa55";

    private readonly PipelineLexer _lexer;
    private readonly string _jobPath;

    public ExportJobReferenceTests()
    {
        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        foreach (var def in new PipelineOptionsCliContributor().GetFlagDefs())
            registry.Register(def with { Stage = FlagStage.All });
        _lexer = new PipelineLexer(registry);

        Environment.SetEnvironmentVariable(Variable, Secret);
        _jobPath = Path.Combine(Path.GetTempPath(), $"dtpipe-export-ref-{Guid.NewGuid():N}.yaml");
        File.WriteAllText(_jobPath,
            "main:\n"
            + "  input: \"csv:in.csv\"\n"
            + $"  output: \"mssql:Server=srvB;Database=B;User Id=u;Password=${{{{{Variable}}}}}\"\n");
    }

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(Variable, null);
        try { File.Delete(_jobPath); } catch { /* the test's verdict does not depend on cleanup */ }
    }

    [Fact]
    public void Exporting_A_Job_File_Keeps_The_Reference_Its_Author_Wrote()
    {
        var jobs = Convert("--job", _jobPath, "--export-job", "out.yaml");

        Assert.Contains($"${{{{{Variable}}}}}", jobs["main"].Output);
        Assert.DoesNotContain(Secret, jobs["main"].Output);
    }

    [Fact]
    public void Running_A_Job_File_Still_Resolves_The_Reference()
    {
        var jobs = Convert("--job", _jobPath);

        Assert.Contains(Secret, jobs["main"].Output);
    }

    private System.Collections.Generic.Dictionary<string, DtPipe.Core.Models.JobDefinition> Convert(params string[] args)
        => PipelineToJobConverter.Convert(_lexer.Parse(args)).Jobs;
}
