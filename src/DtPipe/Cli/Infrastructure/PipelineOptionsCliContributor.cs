using System.CommandLine;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Models;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Contributes CLI flag definitions for universal pipeline engine controls declared in PipelineOptions.
/// Ensures the lexer knows about --batch-size, --limit, --sampling-rate, --sampling-seed.
/// All adapter-specific flags (--key, --query, --table, --strict-schema, --pre-exec, etc.)
/// are contributed by their respective adapter options classes via ICliContributor.GetFlagDefs().
/// </summary>
public class PipelineOptionsCliContributor : ICliContributor
{
    public string Category => "Pipeline Options";

    public IEnumerable<Option> GetCliOptions()
        => CliOptionBuilder.GenerateOptionsForType(typeof(PipelineOptions));

    public IEnumerable<FlagDef> GetFlagDefs()
    {
        foreach (var flag in CliOptionBuilder.GenerateFlagDefsForType(typeof(PipelineOptions), FlagScope.PerBranch))
            yield return flag;

        // --prefix/-p cannot be declared as an instance property on PipelineOptions due to the
        // static Prefix member required by IOptionSet. Contributed manually here.
        yield return new FlagDef("--prefix", new[] { "-p" }, FlagArity.Scalar, FlagScope.PerBranch, "Prefix for all output tables");
    }
}
