using DtPipe.Cli.Validation;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Dag;

/// <summary>
/// Pre-parses the raw application arguments to detect and split the pipeline into a Directed Acyclic Graph (DAG)
/// consisting of multiple branches.
///
/// Split triggers (start a new branch):
///   - <c>-i</c> / <c>--input</c>  when a previous input was already seen in the current branch
///   - <c>--from</c>               always (fan-out consumer or stream transformer main source)
///
/// Stream transformer branches:
///   - A branch containing <c>--sql "&lt;query&gt;"</c> (value processor flag) activates the SQL processor.
///   - A branch containing <c>--merge</c> (boolean processor flag, no value) activates the merge processor.
///   - <c>--from a,b,c</c> declares streaming upstream sources (comma-separated for multiple).
///   - <c>--ref a,b</c>    declares materialized reference sources (comma-separated, lookup/join).
/// </summary>
public static class CliDagParser
{
    /// <summary>
    /// Parses the raw command line arguments into a JobDagDefinition.
    /// </summary>
    public static JobDagDefinition Parse(string[] args, string? defaultProcessor = null)
    {
        if (args == null || args.Length == 0)
            return new JobDagDefinition { Branches = Array.Empty<BranchDefinition>() };

        var branches = new List<BranchDefinition>();
        var currentBranchArgs = new List<string>();
        bool hasSeenInputInCurrentBranch = false;
        int branchCounter = 1;

        // Pre-collect all explicit --alias values so the auto-counter never collides with them.
        var explicitAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (args[i].Equals("--alias", StringComparison.OrdinalIgnoreCase))
                explicitAliases.Add(args[i + 1]);
        }

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            if (CliPipelineRules.InputFlags.Contains(arg))
            {
                // Split only if we already have an input in the current branch.
                if (hasSeenInputInCurrentBranch && currentBranchArgs.Count > 0)
                {
                    branches.Add(CreateBranch(currentBranchArgs, ref branchCounter, explicitAliases));
                    currentBranchArgs.Clear();
                    hasSeenInputInCurrentBranch = false;
                }

                hasSeenInputInCurrentBranch = true;
                currentBranchArgs.Add(arg);
            }
            else if (arg.Equals("--from", StringComparison.OrdinalIgnoreCase))
            {
                // --from always starts a new branch (fan-out or stream-transformer main source).
                if (currentBranchArgs.Count > 0)
                {
                    branches.Add(CreateBranch(currentBranchArgs, ref branchCounter, explicitAliases));
                    currentBranchArgs.Clear();
                }

                hasSeenInputInCurrentBranch = false;
                currentBranchArgs.Add(arg);
            }
            else
            {
                currentBranchArgs.Add(arg);
            }
        }

        if (currentBranchArgs.Count > 0)
            branches.Add(CreateBranch(currentBranchArgs, ref branchCounter, explicitAliases));

        return new JobDagDefinition { Branches = branches };
    }

    private static BranchDefinition CreateBranch(List<string> args, ref int branchCounter, HashSet<string> explicitAliases)
    {
        var argsArray = args.ToArray();

        string? alias = ExtractArgValue(argsArray, "--alias");
        if (string.IsNullOrEmpty(alias))
        {
            // Skip counter values that collide with explicit --alias names defined elsewhere in the DAG.
            while (explicitAliases.Contains($"stream{branchCounter}"))
                branchCounter++;
            alias = $"stream{branchCounter}";
        }
        branchCounter++;

        // --from accepts comma-separated aliases: --from a,b,c
        var fromValue = ExtractArgValue(argsArray, "--from");
        var streamingAliases = fromValue != null
            ? fromValue.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : Array.Empty<string>();

        // --ref accepts comma-separated aliases: --ref a,b
        var refAliases = ExtractAllArgValues(argsArray, "--ref")
            .SelectMany(r => r.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .ToList();

        // Processor detection: boolean flags (e.g. --merge) take precedence, then value flags (e.g. --sql).
        var processorName =
            argsArray.FirstOrDefault(a => CliPipelineRules.BooleanProcessorFlags.Contains(a))?.TrimStart('-') ??
            argsArray.FirstOrDefault(a => CliPipelineRules.ValueProcessorFlags.Contains(a))?.TrimStart('-');

        // Input: for stream-transformer branches there is no injected -i.
        // For regular branches, extract the explicit -i / --input value.
        string? input = ExtractArgValue(argsArray, "-i") ?? ExtractArgValue(argsArray, "--input");

        return new BranchDefinition
        {
            Alias = alias,
            Arguments = argsArray,
            Input = input,
            Output = ExtractArgValue(argsArray, "-o") ?? ExtractArgValue(argsArray, "--output"),
            StreamingAliases = streamingAliases,
            RefAliases = refAliases,
            ProcessorName = processorName
        };
    }

    /// <summary>
    /// Validates the DAG semantics.
    /// Combines CLI-specific rules (singletons) with core structural rules.
    /// Pass <paramref name="processorFactories"/> to enable processor capability validation
    /// (stream/lookup count constraints).
    /// </summary>
    public static IReadOnlyList<string> Validate(JobDagDefinition dag,
        IEnumerable<DtPipe.Core.Abstractions.IStreamTransformerFactory>? processorFactories = null)
    {
        var errors = new List<string>();

        // 1. Structural Validation (Core)
        errors.AddRange(DtPipe.Core.Validation.DagValidator.Validate(dag, processorFactories));

        // 2. CLI-Specific Validation (Singleton flags)
        foreach (var branch in dag.Branches)
        {
            var seenSingletons = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var arg in branch.Arguments)
            {
                if (CliPipelineRules.IsSingleton(arg))
                {
                    if (seenSingletons.Contains(arg))
                        errors.Add($"Branch '{branch.Alias}' contains multiple instances of singleton flag '{arg}'. Only one is allowed.");
                    seenSingletons.Add(arg);
                }
            }

            // 3. Range Validation (e.g. --limit)
            var limit = ExtractArgValue(branch.Arguments, "--limit");
            if (!string.IsNullOrEmpty(limit) && int.TryParse(limit, out int l) && l < 0)
                errors.Add($"Branch '{branch.Alias}' has an invalid --limit value: {limit}. Must be non-negative.");
        }

        return errors;
    }

    public static string? ExtractArgValue(string[] args, string flag)
    {
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (args[i].Equals(flag, StringComparison.OrdinalIgnoreCase))
            {
                var val = args[i + 1];
                if (val.StartsWith('-') && val.Length > 1 && !char.IsDigit(val[1])) return null;
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
