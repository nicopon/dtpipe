using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Processors.Merge;

/// <summary>
/// Factory for <see cref="MergeTransformer"/>.
/// Activated when branch arguments contain the boolean flag <c>--merge</c> (no value).
/// Streaming sources are declared via <c>--from a,b,c</c> (comma-separated).
/// </summary>
public class MergeTransformerFactory : IStreamTransformerFactory
{
    public string ComponentName => "merge";
    public string Category => "Stream Processors";
    public bool RequiresArrowChannels => true;

    public int MinStreams => 2;
    public int MaxStreams => -1;
    public int MinLookups => 0;
    public int MaxLookups => 0;

    public bool IsApplicable(string[] branchArgs)
        => branchArgs.Any(a => a.Equals("--merge", StringComparison.OrdinalIgnoreCase));

    public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        var fromValue = BranchArgParser.ExtractValue(branchArgs, "--from")
            ?? throw new ArgumentException("--from <aliases> is required for MergeTransformer");

        // Parse comma-separated streaming aliases and resolve logical→physical via AliasMap.
        var aliases = fromValue
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(a => ctx.AliasMap.GetValueOrDefault(a, a))
            .ToList();

        if (aliases.Count < 2)
            throw new ArgumentException($"MergeTransformer requires at least 2 streaming sources via '--from a,b,...', got: {fromValue}");

        var registry = serviceProvider.GetRequiredService<IArrowChannelRegistry>();
        return new MergeTransformer(registry, aliases);
    }
}
