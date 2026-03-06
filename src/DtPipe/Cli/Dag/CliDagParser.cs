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
    /// <returns>A parsed DAG definition containing global arguments and specific branches.</returns>
    public static JobDagDefinition Parse(string[] args)
    {
        if (args == null || args.Length == 0)
        {
            return new JobDagDefinition { Branches = Array.Empty<BranchDefinition>() };
        }

        var branches = new List<BranchDefinition>();
        var currentBranchArgs = new List<string>();

        int startAt = 0;

        bool isCurrentBranchXStreamer = false;
        bool hasSeenInputInCurrentBranch = false;
        int branchCounter = 0;

        for (int i = startAt; i < args.Length; i++)
        {
            var arg = args[i];

            if (CliPipelineRules.XStreamerFlags.Contains(arg))
            {
                // We've hit an XStreamer boundary. Finish the current branch.
                if (currentBranchArgs.Count > 0)
                {
                    branches.Add(CreateBranch(currentBranchArgs, isCurrentBranchXStreamer, ref branchCounter));
                    currentBranchArgs.Clear();
                }

                // Start a new XStreamer branch
                isCurrentBranchXStreamer = true;
                hasSeenInputInCurrentBranch = false;
                currentBranchArgs.Add(arg);
            }
            else if (CliPipelineRules.InputFlags.Contains(arg))
            {
                // Split if we've already seen an input (linear sequence)
                // OR if we are switching from an XStreamer branch to a linear branch.
                if (hasSeenInputInCurrentBranch || isCurrentBranchXStreamer)
                {
                    if (currentBranchArgs.Count > 0)
                    {
                        branches.Add(CreateBranch(currentBranchArgs, isCurrentBranchXStreamer, ref branchCounter));
                        currentBranchArgs.Clear();
                    }
                }

                isCurrentBranchXStreamer = false; // New branch started with -i is linear
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
            branches.Add(CreateBranch(currentBranchArgs, isCurrentBranchXStreamer, ref branchCounter));
        }

        return new JobDagDefinition
        {
            Branches = branches
        };
    }

    private static BranchDefinition CreateBranch(List<string> args, bool isXStreamer, ref int branchCounter)
    {
        string? alias = ExtractArgValue(args.ToArray(), "--alias");

        if (string.IsNullOrEmpty(alias))
        {
            alias = $"stream{branchCounter}";
        }

        branchCounter++;

        var argsArray = args.ToArray();
        return new BranchDefinition
        {
            Alias = alias,
            Arguments = argsArray,
            IsXStreamer = isXStreamer,
            Input = ExtractArgValue(argsArray, "-i") ?? ExtractArgValue(argsArray, "--input"),
            Output = ExtractArgValue(argsArray, "-o") ?? ExtractArgValue(argsArray, "--output"),
            MainAlias = ExtractArgValue(argsArray, "--main"),
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
            if (args[i].Equals(flag, StringComparison.OrdinalIgnoreCase)) return args[i + 1];
        return null;
    }

    private static IEnumerable<string> ExtractAllArgValues(string[] args, string flag)
    {
        for (int i = 0; i < args.Length - 1; i++)
            if (args[i].Equals(flag, StringComparison.OrdinalIgnoreCase)) yield return args[i + 1];
    }
}
