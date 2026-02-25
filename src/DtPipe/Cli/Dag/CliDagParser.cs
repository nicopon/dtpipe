using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Dag;

/// <summary>
/// Pre-parses the raw application arguments to detect and split the pipeline into a Directed Acyclic Graph (DAG)
/// consisting of multiple branches separated by the '-x' or '--xstreamer' flags.
/// </summary>
public static class CliDagParser
{
    private static readonly string[] XStreamerFlags = { "-x", "--xstreamer" };
    private static readonly string[] AliasFlags = { "--alias" };
    private static readonly string[] InputFlags = { "-i", "--input" };

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
        if (args.Length > 0 && args[0].Equals("dag", StringComparison.OrdinalIgnoreCase))
        {
            startAt = 1;
        }

        bool isCurrentBranchXStreamer = false;
        bool hasSeenInputInCurrentBranch = false;
        int branchCounter = 0;

        for (int i = startAt; i < args.Length; i++)
        {
            var arg = args[i];

            if (XStreamerFlags.Contains(arg, StringComparer.OrdinalIgnoreCase))
            {
                // We've hit an XStreamer boundary. Finish the current branch (if any arguments exist).
                if (currentBranchArgs.Count > 0 || branches.Count == 0) // Allow empty first branch if started directly with -x
                {
                    branches.Add(CreateBranch(currentBranchArgs, isCurrentBranchXStreamer, ref branchCounter));
                    currentBranchArgs.Clear();
                }

                // Start a new XStreamer branch
                isCurrentBranchXStreamer = true;
                hasSeenInputInCurrentBranch = false;
                currentBranchArgs.Add(arg); // Include the -x flag in the branch arguments so the child parser can identify the provider
            }
            else if (InputFlags.Contains(arg, StringComparer.OrdinalIgnoreCase))
            {
                // If we've already seen an input flag in this branch, it means we're dealing with a new linear branch implicitly
                if (hasSeenInputInCurrentBranch)
                {
                    if (currentBranchArgs.Count > 0)
                    {
                        branches.Add(CreateBranch(currentBranchArgs, isCurrentBranchXStreamer, ref branchCounter));
                        currentBranchArgs.Clear();
                    }
                    isCurrentBranchXStreamer = false;
                }

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
        string? alias = ExtractAlias(args);

        if (string.IsNullOrEmpty(alias))
        {
            alias = $"stream{branchCounter}";
        }

        branchCounter++;

        return new BranchDefinition
        {
            Alias = alias,
            Arguments = args.ToArray(),
            IsXStreamer = isXStreamer
        };
    }

    private static string? ExtractAlias(List<string> args)
    {
        for (int i = 0; i < args.Count; i++)
        {
            if (AliasFlags.Contains(args[i], StringComparer.OrdinalIgnoreCase) && i + 1 < args.Count)
            {
                // Note: We might want to remove the --alias flag from the args list if it's strictly a DAG-level concept,
                // but for now we leave it so the underlying System.CommandLine doesn't fail if we define it as an option.
                return args[i + 1];
            }
        }
        return null;
    }

    /// <summary>
    /// Validates the DAG topology. Returns a list of error messages. Empty = valid.
    /// </summary>
    public static IReadOnlyList<string> Validate(JobDagDefinition dag)
    {
        var errors = new List<string>();
        var registeredAliases = new HashSet<string>(dag.Branches.Select(b => b.Alias), StringComparer.OrdinalIgnoreCase);

        foreach (var branch in dag.Branches.Where(b => b.IsXStreamer))
        {
            // Extract --main value from branch.Arguments
            var mainAlias = ExtractArgValue(branch.Arguments, "--main");
            if (string.IsNullOrEmpty(mainAlias))
            {
                errors.Add($"XStreamer branch '{branch.Alias}' is missing the '--main' argument.");
            }
            else if (!registeredAliases.Contains(mainAlias))
            {
                errors.Add($"XStreamer branch '{branch.Alias}' references unknown alias '--main {mainAlias}'. Known aliases: {string.Join(", ", registeredAliases)}.");
            }

            // Extract all --ref values
            var rawRefAliases = ExtractAllArgValues(branch.Arguments, "--ref");
            foreach (var rawRef in rawRefAliases)
            {
                var refAliases = rawRef.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                foreach (var refAlias in refAliases)
                {
                    if (!registeredAliases.Contains(refAlias))
                    {
                        errors.Add($"XStreamer branch '{branch.Alias}' references unknown alias '--ref {refAlias}'. Known aliases: {string.Join(", ", registeredAliases)}.");
                    }
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
