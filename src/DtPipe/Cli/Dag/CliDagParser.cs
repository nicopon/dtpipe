using DtPipe.Cli.Validation;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Dag;

/// <summary>
/// Pre-parses the raw application arguments to detect and split the pipeline into a Directed Acyclic Graph (DAG)
/// consisting of multiple branches separated by the '-x' or '--xstreamer' flags.
/// </summary>
public static class CliDagParser
{
    // Use flags from CliPipelineRules
    // private static readonly string[] XStreamerFlags = { "-x", "--xstreamer" };
    // private static readonly string[] AliasFlags = { "--alias" };
    // private static readonly string[] InputFlags = { "-i", "--input" };

    /// <summary>
    /// Parses the raw command line arguments into a JobDagDefinition.
    /// </summary>
    /// <param name="args">The raw args from Environment.GetCommandLineArgs() (excluding the executable path).</param>
    /// <param name="defaultXStreamer">Optional default XStreamer if unspecified.</param>
    /// <returns>A parsed DAG definition containing global arguments and specific branches.</returns>
    public static JobDagDefinition Parse(string[] args, string? defaultXStreamer = null)
    {
        if (args == null || args.Length == 0)
        {
            return new JobDagDefinition { Branches = Array.Empty<BranchDefinition>() };
        }

        var branches = new List<BranchDefinition>();
        var currentBranchArgs = new List<string>();

        bool isCurrentBranchXStreamer = false;
        bool hasSeenInputInCurrentBranch = false;
        int branchCounter = 0;

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            if (CliPipelineRules.XStreamerFlags.Contains(arg))
            {
                // We've hit an XStreamer boundary. Finish the current branch.
                if (currentBranchArgs.Count > 0)
                {
                    var branch = CreateBranch(currentBranchArgs, isCurrentBranchXStreamer, ref branchCounter, defaultXStreamer, branches.LastOrDefault()?.Alias);
                    branches.Add(branch);
                    currentBranchArgs.Clear();
                }

                // Start a new XStreamer branch
                isCurrentBranchXStreamer = true;
                hasSeenInputInCurrentBranch = false;
                currentBranchArgs.Add(arg);
            }
            else if (CliPipelineRules.InputFlags.Contains(arg) || arg.Equals("--from", StringComparison.OrdinalIgnoreCase))
            {
                // Split if we've already seen an input in the current branch OR if switching away from an XStreamer branch.
                // Note: --main and --ref no longer trigger branch splits here; they are XStreamer-only flags.
                if (hasSeenInputInCurrentBranch || isCurrentBranchXStreamer)
                {
                    if (currentBranchArgs.Count > 0)
                    {
                        var branch = CreateBranch(currentBranchArgs, isCurrentBranchXStreamer, ref branchCounter, defaultXStreamer, branches.LastOrDefault()?.Alias);
                        branches.Add(branch);
                        currentBranchArgs.Clear();
                    }
                }

                isCurrentBranchXStreamer = false; // New branch started with -i or --from is always linear
                hasSeenInputInCurrentBranch = true;
                currentBranchArgs.Add(arg);
            }
            else
            {
                currentBranchArgs.Add(arg);
            }
        }

        // Add the last branch
        if (currentBranchArgs.Count > 0)
        {
            branches.Add(CreateBranch(currentBranchArgs, isCurrentBranchXStreamer, ref branchCounter, defaultXStreamer, branches.LastOrDefault()?.Alias));
        }

        return new JobDagDefinition
        {
            Branches = branches
        };
    }

    private static BranchDefinition CreateBranch(List<string> args, bool isXStreamer, ref int branchCounter, string? defaultXStreamer = null, string? previousAlias = null)
    {
        string? alias = ExtractArgValue(args.ToArray(), "--alias");

        if (string.IsNullOrEmpty(alias))
        {
            alias = $"stream{branchCounter}";
        }

        branchCounter++;

        var argsArray = args.ToArray();
        var input = ExtractArgValue(argsArray, "-i") ?? ExtractArgValue(argsArray, "--input");

        if (input == null && isXStreamer)
        {
            input = ExtractArgValue(argsArray, "--xstreamer") ?? ExtractArgValue(argsArray, "-x") ?? defaultXStreamer;
        }

        var mainAlias = ExtractArgValue(argsArray, "--main");
        if (isXStreamer && string.IsNullOrEmpty(mainAlias) && !string.IsNullOrEmpty(previousAlias))
        {
            mainAlias = previousAlias;
        }

        // Extract the --from alias for fan-out (tee) branches.
        // This is mutually exclusive with having an explicit -i input.
        var fromAlias = ExtractArgValue(argsArray, "--from");

        return new BranchDefinition
        {
            Alias = alias,
            Arguments = argsArray,
            IsXStreamer = isXStreamer,
            Input = input,
            Output = ExtractArgValue(argsArray, "-o") ?? ExtractArgValue(argsArray, "--output"),
            MainAlias = mainAlias,
            FromAlias = fromAlias,
            RefAliases = ExtractAllArgValues(argsArray, "--ref")
                .SelectMany(r => r.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                .ToList()
        };
    }

    /// <summary>
    /// Validates the DAG semantics.
    /// Combines CLI-specific rules (singletons) with core structural rules.
    /// </summary>
    public static IReadOnlyList<string> Validate(JobDagDefinition dag)
    {
        var errors = new List<string>();

        // 1. Structural Validation (Core)
        errors.AddRange(DtPipe.Core.Validation.DagValidator.Validate(dag));

        // 2. CLI-Specific Validation (Singleton flags)
        foreach (var branch in dag.Branches)
        {
            var seenSingletons = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var arg in branch.Arguments)
            {
                if (CliPipelineRules.IsSingleton(arg))
                {
                    if (seenSingletons.Contains(arg))
                    {
                        errors.Add($"Branch '{branch.Alias}' contains multiple instances of singleton flag '{arg}'. Only one is allowed.");
                    }
                    seenSingletons.Add(arg);
                }
            }
        }

        return errors;
    }

    private static string? ExtractArgValue(string[] args, string flag)
    {
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (args[i].Equals(flag, StringComparison.OrdinalIgnoreCase))
            {
                var val = args[i + 1];
                if (val.StartsWith('-')) return null; // Looks like another flag
                return val;
            }
        }
        return null;
    }

    private static IEnumerable<string> ExtractAllArgValues(string[] args, string flag)
    {
        for (int i = 0; i < args.Length - 1; i++)
            if (args[i].Equals(flag, StringComparison.OrdinalIgnoreCase)) yield return args[i + 1];
    }
}
