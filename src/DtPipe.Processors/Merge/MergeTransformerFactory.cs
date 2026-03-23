using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Processors.Merge;

/// <summary>
/// Factory for <see cref="MergeTransformer"/>.
/// Activated when branch arguments contain <c>--merge &lt;alias&gt;</c>.
/// The main channel is taken from <c>--from &lt;alias&gt;</c>.
/// </summary>
public class MergeTransformerFactory : IStreamTransformerFactory
{
    public string ComponentName => "merge";
    public string Category => "Stream Processors";
    public bool RequiresArrowChannels => true;

    public bool IsApplicable(string[] branchArgs)
        => ExtractArgValue(branchArgs, "--merge") != null;

    public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        // For merge branches the orchestrator rewrites --from/--merge to physical aliases directly,
        // so the values read here are already the correct physical channel aliases.
        var mainAlias = ExtractArgValue(branchArgs, "--from")
            ?? throw new ArgumentException("--from <alias> is required for MergeTransformer");
        var mergeAlias = ExtractArgValue(branchArgs, "--merge")
            ?? throw new ArgumentException("--merge <alias> is required for MergeTransformer");

        var registry = serviceProvider.GetRequiredService<IArrowChannelRegistry>();
        return new MergeTransformer(registry, mainAlias, mergeAlias);
    }

    private static string? ExtractArgValue(string[] args, string flag)
    {
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (!args[i].Equals(flag, StringComparison.OrdinalIgnoreCase)) continue;
            var val = args[i + 1];
            if (val.StartsWith('-') && val.Length > 1 && !char.IsDigit(val[1])) return null;
            return val;
        }
        return null;
    }
}
