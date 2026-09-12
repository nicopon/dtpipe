using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// F7 — the engine-settings bundle: its defaults, and the single derivation step that folds it
/// into a job.
/// </summary>
public class BranchEngineSettingsTests
{
    [Fact]
    public void EngineSettings_Defaults_Are_Sane()
    {
        var d = BranchEngineSettings.Default;
        Assert.Equal(0, d.Limit);
        Assert.Equal(DtPipe.Core.Models.PipelineOptions.DefaultBatchSize, d.BatchSize);
        Assert.Equal(1.0, d.SamplingRate);
        Assert.Null(d.SamplingSeed);
        Assert.False(d.NoStats);
    }

    [Fact]
    public void ApplyTo_Overrides_Engine_Fields_But_Preserves_Provider_Data()
    {
        var job = new JobDefinition
        {
            Input = "csv:in.csv",
            Output = "csv:out.csv",
            Limit = 99,
            MetricsPath = "from-job.json",
        };
        var engine = BranchEngineSettings.Default with { Limit = 42 };

        var applied = engine.ApplyTo(job);

        Assert.Equal(42, applied.Limit);
        Assert.Equal("csv:in.csv", applied.Input);
        Assert.Equal("csv:out.csv", applied.Output);
        // null engine metrics must not clobber the job's own value
        Assert.Equal("from-job.json", applied.MetricsPath);
    }

    [Fact]
    public void ApplyTo_NoStats_Is_Additive()
    {
        var job = new JobDefinition { NoStats = true };
        var applied = BranchEngineSettings.Default.ApplyTo(job);
        Assert.True(applied.NoStats);
    }
}
