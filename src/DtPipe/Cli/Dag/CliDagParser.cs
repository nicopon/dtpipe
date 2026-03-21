using DtPipe.Cli.Validation;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Dag;

/// <summary>
/// Pre-parses the raw application arguments to detect and split the pipeline into a Directed Acyclic Graph (DAG)
/// consisting of multiple branches.
///
/// Split triggers (start a new branch):
///   - <c>-i</c> / <c>--input</c>  when a previous input was already seen in the current branch
///   - <c>--from</c>               always (fan-out consumer or processor main source)
///
/// A branch containing <c>--sql</c> is identified as a SQL processor:
///   - <c>--from &lt;alias&gt;</c> declares the main streaming source.
///   - <c>--ref &lt;alias&gt;</c>  declares materialized reference sources.
///   - <c>--sql "&lt;query&gt;"</c> carries the SQL query inline.
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
        int branchCounter = 0;

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            if (CliPipelineRules.InputFlags.Contains(arg))
            {
                // Split only if we already have an input in the current branch.
                if (hasSeenInputInCurrentBranch && currentBranchArgs.Count > 0)
                {
                    branches.Add(CreateBranch(currentBranchArgs, ref branchCounter, defaultProcessor));
                    currentBranchArgs.Clear();
                    hasSeenInputInCurrentBranch = false;
                }

                hasSeenInputInCurrentBranch = true;
                currentBranchArgs.Add(arg);
            }
            else if (arg.Equals("--from", StringComparison.OrdinalIgnoreCase))
            {
                // --from always starts a new branch (fan-out consumer or processor main source).
                if (currentBranchArgs.Count > 0)
                {
                    branches.Add(CreateBranch(currentBranchArgs, ref branchCounter, defaultProcessor));
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
            branches.Add(CreateBranch(currentBranchArgs, ref branchCounter, defaultProcessor));

        return new JobDagDefinition { Branches = branches };
    }

    private static BranchDefinition CreateBranch(List<string> args, ref int branchCounter, string? defaultProcessor = null)
    {
        var argsArray = args.ToArray();

        string? alias = ExtractArgValue(argsArray, "--alias");
        if (string.IsNullOrEmpty(alias))
            alias = $"stream{branchCounter}";
        branchCounter++;

        var sqlQuery = ExtractSqlQuery(argsArray);
        bool isProcessor = sqlQuery != null;

        var fromValue = ExtractArgValue(argsArray, "--from");

        var refAliases = ExtractAllArgValues(argsArray, "--ref")
            .SelectMany(r => r.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .ToList();

        string? mainAlias = null;
        string? fromAlias = null;
        string? input;
        string[] normalizedArgs;

        if (isProcessor)
        {
            mainAlias = fromValue;
            input = defaultProcessor ?? "fusion-engine";
            // Normalize: --from <alias> → --main <alias>; --sql "<query>" → -q "<query>"
            // The orchestrator injects --sql <engine> when --sql is absent from the args.
            normalizedArgs = NormalizeProcessorArgs(argsArray, mainAlias);
        }
        else
        {
            input = ExtractArgValue(argsArray, "-i") ?? ExtractArgValue(argsArray, "--input");
            fromAlias = fromValue;
            normalizedArgs = argsArray;
        }

        return new BranchDefinition
        {
            Alias = alias,
            Arguments = normalizedArgs,
            Processor = isProcessor ? ProcessorKind.Sql : ProcessorKind.None,
            Input = input,
            Output = ExtractArgValue(argsArray, "-o") ?? ExtractArgValue(argsArray, "--output"),
            MainAlias = mainAlias,
            FromAlias = fromAlias,
            RefAliases = refAliases,
            SqlQuery = sqlQuery
        };
    }

    /// <summary>
    /// Extracts the SQL query from <c>--sql "&lt;query&gt;"</c>.
    /// Returns null if no <c>--sql</c> flag is present (branch is not a processor).
    /// </summary>
    private static string? ExtractSqlQuery(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (!args[i].Equals("--sql", StringComparison.OrdinalIgnoreCase)) continue;

            if (i + 1 >= args.Length) return null;

            var val = args[i + 1];
            // Next token is another flag → no value.
            if (val.StartsWith('-') && val.Length > 1 && !char.IsDigit(val[1])) return null;

            return val;
        }
        return null;
    }

    /// <summary>
    /// Normalizes the raw args of a processor branch for System.CommandLine / DataFusionProcessorFactory:
    /// <list type="bullet">
    ///   <item><c>--from &lt;alias&gt;</c> → <c>--main &lt;alias&gt;</c></item>
    ///   <item><c>--sql "&lt;query&gt;"</c> → <c>-q "&lt;query&gt;"</c>
    ///         (the orchestrator injects <c>--sql fusion-engine</c> when the flag is absent)</item>
    /// </list>
    /// </summary>
    private static string[] NormalizeProcessorArgs(string[] args, string? mainAlias)
    {
        var result = new List<string>(args.Length);
        int i = 0;

        while (i < args.Length)
        {
            var arg = args[i];

            if (arg.Equals("--from", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                result.Add("--main");
                result.Add(args[i + 1]);
                i += 2;
            }
            else if (arg.Equals("--sql", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                // --sql "<query>" → -q "<query>"; orchestrator will inject --sql <engine>.
                result.Add("-q");
                result.Add(args[i + 1]);
                i += 2;
            }
            else
            {
                result.Add(arg);
                i++;
            }
        }

        return result.ToArray();
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

    private static string? ExtractArgValue(string[] args, string flag)
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
