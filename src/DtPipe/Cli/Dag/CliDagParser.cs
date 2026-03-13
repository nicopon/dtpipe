using DtPipe.Cli.Validation;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Dag;

/// <summary>
/// Pre-parses the raw application arguments to detect and split the pipeline into a Directed Acyclic Graph (DAG)
/// consisting of multiple branches separated by the '--sql', '-x' or '--processor' flags.
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
    /// <param name="defaultProcessor">Optional default processor if unspecified.</param>
    /// <returns>A parsed DAG definition containing global arguments and specific branches.</returns>
    public static JobDagDefinition Parse(string[] args, string? defaultProcessor = null)
    {
        if (args == null || args.Length == 0)
        {
            return new JobDagDefinition { Branches = Array.Empty<BranchDefinition>() };
        }

        var branches = new List<BranchDefinition>();
        var currentBranchArgs = new List<string>();

        bool isCurrentBranchProcessor = false;
        bool hasSeenInputInCurrentBranch = false;
        int branchCounter = 0;

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            if (CliPipelineRules.ProcessorFlags.Contains(arg))
            {
                // Split if we already have a processor OR an input in the current branch
                if (isCurrentBranchProcessor || hasSeenInputInCurrentBranch)
                {
                    if (currentBranchArgs.Count > 0)
                    {
                        var branch = CreateBranch(currentBranchArgs, isCurrentBranchProcessor, ref branchCounter, defaultProcessor, branches.LastOrDefault()?.Alias);
                        branches.Add(branch);
                        currentBranchArgs.Clear();
                    }
                    hasSeenInputInCurrentBranch = false;
                }

                isCurrentBranchProcessor = true;
                currentBranchArgs.Add(arg);
            }
            else if (CliPipelineRules.InputFlags.Contains(arg) || arg.Equals("--from", StringComparison.OrdinalIgnoreCase))
            {
                // Split if we've already seen an input in the current branch.
                if (hasSeenInputInCurrentBranch)
                {
                    if (currentBranchArgs.Count > 0)
                    {
                        var branch = CreateBranch(currentBranchArgs, isCurrentBranchProcessor, ref branchCounter, defaultProcessor, branches.LastOrDefault()?.Alias);
                        branches.Add(branch);
                        currentBranchArgs.Clear();
                    }
                    isCurrentBranchProcessor = false;
                }

                hasSeenInputInCurrentBranch = true;
                currentBranchArgs.Add(arg);
            }
            else if (arg.Equals("--main", StringComparison.OrdinalIgnoreCase) || arg.Equals("--ref", StringComparison.OrdinalIgnoreCase))
            {
                // Split on --main/--ref ONLY IF we are NOT in a processor branch AND already have an input.
                // If we ARE in a processor branch, these are just options.
                if (!isCurrentBranchProcessor && hasSeenInputInCurrentBranch)
                {
                    if (currentBranchArgs.Count > 0)
                    {
                        var branch = CreateBranch(currentBranchArgs, isCurrentBranchProcessor, ref branchCounter, defaultProcessor, branches.LastOrDefault()?.Alias);
                        branches.Add(branch);
                        currentBranchArgs.Clear();
                    }
                    hasSeenInputInCurrentBranch = false; 
                }
                
                if (!isCurrentBranchProcessor) hasSeenInputInCurrentBranch = true;
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
            branches.Add(CreateBranch(currentBranchArgs, isCurrentBranchProcessor, ref branchCounter, defaultProcessor, branches.LastOrDefault()?.Alias));
        }

        return new JobDagDefinition
        {
            Branches = branches
        };
    }

    private static BranchDefinition CreateBranch(List<string> args, bool isProcessor, ref int branchCounter, string? defaultProcessor = null, string? previousAlias = null)
    {
        string? alias = ExtractArgValue(args.ToArray(), "--alias");

        if (string.IsNullOrEmpty(alias))
        {
            alias = $"stream{branchCounter}";
        }

        branchCounter++;

        var argsArray = args.ToArray();
        var input = ExtractArgValue(argsArray, "-i") ?? ExtractArgValue(argsArray, "--input");

        if (input == null && isProcessor)
        {
            input = ExtractArgValue(argsArray, "--sql") ?? ExtractArgValue(argsArray, "--xstreamer") ?? ExtractArgValue(argsArray, "-x") ?? defaultProcessor;
        }

        var mainAlias = ExtractArgValue(argsArray, "--main");
        if (isProcessor && string.IsNullOrEmpty(mainAlias) && !string.IsNullOrEmpty(previousAlias))
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
            Processor = isProcessor ? ProcessorKind.Sql : ProcessorKind.None,
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

            // 3. Range Validation (e.g. --limit)
            var limit = ExtractArgValue(branch.Arguments, "--limit");
            if (!string.IsNullOrEmpty(limit) && int.TryParse(limit, out int l) && l < 0)
            {
                errors.Add($"Branch '{branch.Alias}' has an invalid --limit value: {limit}. Must be non-negative.");
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
                if (val.StartsWith('-') && val.Length > 1 && !char.IsDigit(val[1])) return null; // Looks like another flag
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
