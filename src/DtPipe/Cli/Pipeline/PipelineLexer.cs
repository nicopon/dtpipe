using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Core.Pipelines;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// Sequential lexer for DtPipe pipelines.
/// Branches are split implicitly by the second occurrence of -i/--input or by any --from flag.
/// All flags belong to the branch in which they appear, with strict stage-scoping enforced by
/// BuildBranch: flags must appear in the correct stage (reader before transformers, writer after -o).
/// Strictness: a non-repeatable flag may appear at most ONCE per stage within a branch, and a
/// global scalar flag at most once per command line — duplicates are hard errors, not warnings
/// (the previously silent last-wins behavior made the executed configuration ambiguous).
/// </summary>
public class PipelineLexer
{
    /// <summary>Coarse execution stage currently being lexed; mirrors BuildBranch's slicing.</summary>
    private enum LexStage { Reader, Pipeline, Writer }

    private readonly FlagRegistry _registry;

    public PipelineLexer(FlagRegistry registry)
    {
        _registry = registry;
    }

    public ParsedPipeline Parse(string[] args)
    {
        var globalDict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var branches = new List<BranchSpec>();

        var currentBranchFlags = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var currentBranchArgs = new List<string>();

        // Duplicate-detection state: (flag → stages where it was already consumed) per branch,
        // plus one process-wide set for global scalar flags. Cleared on branch split.
        var seenPerStage = new Dictionary<string, HashSet<LexStage>>(StringComparer.OrdinalIgnoreCase);
        var seenGlobalScalars = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        LexStage currentStage = LexStage.Reader;

        // The unrecognised flag last seen, if it was the immediately preceding token. An unknown
        // flag does not consume a value (it is kept as a boolean in RawArgs for OptionBinder), so
        // whatever follows it arrives here as a bare token.
        string? lastUnknownFlag = null;

        for (int i = 0; i < args.Length; i++)
        {
            var token = args[i];
            var previousUnknownFlag = lastUnknownFlag;
            lastUnknownFlag = null;

            var def = _registry.Lookup(token);
            if (def != null)
            {
                string? value = null;
                if (def.ConsumesNextToken)
                {
                    // F8 arity-driven consumption: scalar/repeatable flags always take the
                    // next token as their value — even dash-leading ones ("-5", "-###-").
                    // This is the same rule OptionBinder.BindCli applies to raw args.
                    if (i + 1 < args.Length)
                        value = args[++i];
                }

                // Implicit branch-split: one pure function decides (F6). State is read
                // from the flags accumulated so far in the current branch.
                var state = new BranchSplitState(
                    HasInput: currentBranchFlags.ContainsKey("--input") || currentBranchFlags.ContainsKey("-i"),
                    HasJob: currentBranchFlags.ContainsKey("--job") || currentBranchFlags.ContainsKey("-j"),
                    HasFrom: currentBranchFlags.ContainsKey("--from"));

                if (BranchSplitDecision.Decide(state, def.Name) != SplitDecision.Stay)
                {
                    branches.Add(BuildBranch(currentBranchFlags, currentBranchArgs));
                    currentBranchFlags = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                    currentBranchArgs  = new List<string>();
                    seenPerStage.Clear();
                    currentStage = LexStage.Reader;
                }

                // Stage transition — mirrors BuildBranch's writer/pipeline boundary detection:
                // first -o/--output opens the writer stage; the first exact-Pipeline flag opens
                // the transformer stage (only while still in the reader stage).
                if (def.Name == "--output")
                    currentStage = LexStage.Writer;
                else if (def.Stage == FlagStage.Pipeline && currentStage == LexStage.Reader)
                    currentStage = LexStage.Pipeline;

                // Global flags go to globalDict only.
                // Per-branch flags go to both globalDict (for global defaults) and the current branch.
                globalDict[def.Name] = value ?? "true";

                if (def.Scope == FlagScope.Global)
                {
                    // --job is a split trigger (multi-job DAGs are legitimate), never a duplicate.
                    if (def.Arity != FlagArity.Repeatable && def.Name != "--job" && !seenGlobalScalars.Add(def.Name))
                        throw new InvalidOperationException(
                            $"Global flag '{def.Name}' appears more than once; a global flag may be specified only once.");
                    continue;
                }

                if (def.Arity != FlagArity.Repeatable && !IsMultiInstanceOption(def))
                {
                    if (!seenPerStage.TryGetValue(def.Name, out var stages))
                    {
                        stages = new HashSet<LexStage>();
                        seenPerStage[def.Name] = stages;
                    }
                    if (!stages.Add(currentStage))
                        throw new InvalidOperationException(
                            $"Flag '{token}' appears more than once in the same branch stage ({currentStage.ToString().ToLowerInvariant()}). " +
                            "Each non-repeatable flag may appear once per stage: reader flags before transformers, " +
                            "writer flags after -o." + AliasListHint(def.Name));
                }

                if (!currentBranchFlags.ContainsKey(def.Name)) currentBranchFlags[def.Name] = new List<string>();
                currentBranchFlags[def.Name].Add(value ?? "true");
                currentBranchArgs.Add(token);
                if (value != null) currentBranchArgs.Add(value);
            }
            else
            {
                if (token.StartsWith('-'))
                {
                    // Unknown flag — store as boolean, captured in RawArgs for OptionBinder.
                    lastUnknownFlag = token;
                    globalDict[token] = "true";
                    if (!seenPerStage.TryGetValue(token, out var unknownStages))
                    {
                        unknownStages = new HashSet<LexStage>();
                        seenPerStage[token] = unknownStages;
                    }
                    if (!unknownStages.Add(currentStage))
                        throw new InvalidOperationException(
                            $"Flag '{token}' appears more than once in the same branch stage ({currentStage.ToString().ToLowerInvariant()}).");
                    if (!currentBranchFlags.ContainsKey(token)) currentBranchFlags[token] = new List<string>();
                    currentBranchFlags[token].Add("true");
                    currentBranchArgs.Add(token);
                }
                else
                {
                    // A bare token right after an unrecognised flag is that flag's value, not a
                    // query. Promoting it made `--ouput out.csv` build a DuckDB branch and fail
                    // with "Parser Error: syntax error at or near 'out.csv'", never naming the
                    // flag that was misspelt.
                    if (previousUnknownFlag != null)
                        throw new InvalidOperationException(
                            $"Unrecognized flag '{previousUnknownFlag}', and '{token}' reads as its value. "
                          + $"Check the spelling, or pass a query explicitly with --sql \"{token}\" "
                          + "if it really is one.");

                    // Positional token (SQL query without --sql flag).
                    // Split the reader into its own branch before the SQL processor branch.
                    if (currentBranchArgs.Count > 0 && !currentBranchFlags.ContainsKey("--from"))
                    {
                        branches.Add(BuildBranch(currentBranchFlags, currentBranchArgs));
                        currentBranchFlags = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                        currentBranchArgs  = new List<string>();
                        seenPerStage.Clear();
                        currentStage = LexStage.Reader;
                    }
                    if (currentBranchFlags.ContainsKey("--sql") ||
                        (seenPerStage.TryGetValue("--sql", out var sqlStages) && !sqlStages.Add(LexStage.Pipeline)))
                    {
                        throw new InvalidOperationException(
                            "SQL query provided more than once in the same branch: a positional query cannot be " +
                            "combined with --sql. Provide a single --sql \"<query>\".");
                    }
                    if (!seenPerStage.ContainsKey("--sql"))
                        seenPerStage["--sql"] = new HashSet<LexStage>();
                    seenPerStage["--sql"].Add(LexStage.Pipeline);
                    if (!currentBranchFlags.ContainsKey("--sql")) currentBranchFlags["--sql"] = new List<string>();
                    currentBranchFlags["--sql"].Add(token);
                    currentBranchArgs.Add("--sql");
                    currentBranchArgs.Add(token);
                }
            }
        }

        if (currentBranchArgs.Count > 0)
            branches.Add(BuildBranch(currentBranchFlags, currentBranchArgs));

        // A job file supplies each branch's reader from YAML, so CLI reader flags are overrides
        // there rather than flags with nothing to bind to.
        bool hasJobFile = globalDict.ContainsKey("--job");
        foreach (var branch in branches)
        {
            RejectOrphanBranch(branch);
            if (!hasJobFile) RejectReaderFlagsWithoutAReader(branch);
        }

        return new ParsedPipeline(MapGlobals(globalDict), branches);
    }

    /// <summary>
    /// Extra sentence for flags whose value is an alias list. Repeating one is the mistake a
    /// reader makes when they mean "and also this alias"; the generic message about stages does
    /// not tell them the spelling that works.
    /// </summary>
    private static string AliasListHint(string flagName) => flagName switch
    {
        "--ref" => " To name several aliases, list them on a single flag: --ref a,b.",
        _ => ""
    };

    /// <summary>
    /// A second '--from' opens a new branch, so '--from a --from b' yields two branches rather than
    /// one branch reading two sources — and the first is left with nothing to do. Writing it that
    /// way is a mistake for 'a,b', and the failure it used to produce named a downstream component
    /// instead of the flag. Consumers of a fan-out ('--from j -o dst') and the stages of a diamond
    /// ('--from s --filter … --alias hi') all carry an alias, an output or a transformer, so only
    /// the empty branch is rejected.
    /// </summary>
    private static void RejectOrphanBranch(BranchSpec branch)
    {
        if (branch.From.Count == 0) return;
        if (!string.IsNullOrEmpty(branch.Output) || !string.IsNullOrEmpty(branch.Alias)) return;
        if (branch.PipelineArgs.Length > 0 || branch.WriterArgs.Length > 0) return;

        // Everything the branch carries is the --from itself and its value.
        var carried = branch.ReaderArgs.Where(a => a is not ("--from" or "--ref")).ToArray();
        if (carried.Length > branch.From.Count + branch.Ref.Count) return;

        throw new InvalidOperationException(
            $"The branch reading from '{string.Join(",", branch.From)}' has no output, no alias and no transformer, " +
            "so it produces nothing. A second '--from' starts a new branch; to read several sources " +
            $"in ONE branch, list them on a single flag: --from {string.Join(",", branch.From)},<other-alias>.");
    }

    /// <summary>
    /// Pipeline-stage transformer options are consumed per INSTANCE by TransformerPipelineBuilder
    /// (a new instance starts at every trigger-flag recurrence), so repeating them configures the
    /// next instance — legitimate, not ambiguous. Stream-processor triggers (--sql) and all
    /// reader/writer/global flags bind once per branch and stay under the duplicate policy.
    /// </summary>
    private static bool IsMultiInstanceOption(FlagDef def)
        => def.Stage == FlagStage.Pipeline && !def.ProcessorTrigger;

    private GlobalOptions MapGlobals(Dictionary<string, object?> dict)
    {
        return new GlobalOptions
        {
            DryRunCount   = GetDryRun(dict),
            NoStats       = dict.ContainsKey("--no-stats"),
            StrictBindings= dict.ContainsKey("--strict-bindings"),
            LogPath       = dict.TryGetValue("--log", out var logVal) ? logVal?.ToString() : null,
            JobFile       = dict.TryGetValue("--job", out var jobVal) ? jobVal?.ToString()
                          : dict.TryGetValue("-j", out var jVal) ? jVal?.ToString() : null,
            ExportJobFile = dict.TryGetValue("--export-job", out var ejVal) ? ejVal?.ToString() : null,
            Session       = dict.TryGetValue("--session", out var sessVal) ? sessVal?.ToString() : null,
            IgnoreNulls   = dict.ContainsKey("--ignore-nulls"),
            AllFlags      = dict
        };
    }

    private int GetDryRun(Dictionary<string, object?> dict)
    {
        if (dict.TryGetValue("--dry-run", out var val))
        {
            if (val is string s && int.TryParse(s, out var i)) return i;
            return 1;
        }
        if (dict.TryGetValue("-dr", out val))
        {
            if (val is string s && int.TryParse(s, out var i)) return i;
            return 1;
        }
        return 0;
    }

    private BranchSpec BuildBranch(Dictionary<string, List<string>> flags, List<string> rawArgs)
    {
        string? GetSingle(params string[] keys)
        {
            foreach (var k in keys)
                if (flags.TryGetValue(k, out var list)) return list.LastOrDefault();
            return null;
        }

        string[] GetList(params string[] keys)
        {
            var result = new List<string>();
            foreach (var k in keys)
                if (flags.TryGetValue(k, out var list)) result.AddRange(list);
            return result.ToArray();
        }

        // ── Stage-scoped arg splitting ─────────────────────────────────────────────
        // writer boundary: first -o / --output
        int writerStart = -1;
        for (int idx = 0; idx < rawArgs.Count; idx++)
            if (rawArgs[idx] == "-o" || rawArgs[idx] == "--output") { writerStart = idx; break; }

        // pipeline boundary: first flag with FlagStage == Pipeline exactly (transformer trigger)
        int pipelineStart = -1;
        int searchEnd = writerStart >= 0 ? writerStart : rawArgs.Count;
        for (int idx = 0; idx < searchEnd; idx++)
        {
            var def = _registry.Lookup(rawArgs[idx]);
            if (def?.Stage == FlagStage.Pipeline) { pipelineStart = idx; break; }
        }

        int readerEnd   = pipelineStart >= 0 ? pipelineStart : (writerStart >= 0 ? writerStart : rawArgs.Count);
        int pipelineEnd = writerStart >= 0 ? writerStart : rawArgs.Count;

        var readerArgs   = rawArgs.Take(readerEnd).ToArray();
        var pipelineArgs = pipelineStart >= 0
            ? rawArgs.Skip(pipelineStart).Take(pipelineEnd - pipelineStart).ToArray()
            : Array.Empty<string>();
        var writerArgs   = writerStart >= 0 ? rawArgs.Skip(writerStart).ToArray() : Array.Empty<string>();

        // ── Stage validation ────────────────────────────────────────────────────────
        ValidateStageConstraints(readerArgs,   FlagStage.Reader,   "before the first transformer or -o");
        ValidateStageConstraints(writerArgs,   FlagStage.Writer,   "after -o");
        ValidateStageConstraints(pipelineArgs, FlagStage.Pipeline, "in transformer scope (between transformers and -o)");

        return new BranchSpec
        {
            Input  = GetSingle("--input", "-i"),
            Output = GetSingle("--output", "-o"),
            Alias  = GetSingle("--alias"),
            From   = GetList("--from").SelectMany(s => s.Split(',')).Select(s => s.Trim()).ToList(),
            Ref    = GetList("--ref").SelectMany(s => s.Split(',')).Select(s => s.Trim()).ToList(),

            ReaderArgs   = readerArgs,
            PipelineArgs = pipelineArgs,
            WriterArgs   = writerArgs,

            RawArgs = rawArgs.ToArray(),
            Flags   = flags
        };
    }

    /// <summary>
    /// A branch opened by '--from' has no reader of its own, so a flag that is not valid in the
    /// pipeline stage has nothing to bind to. Such a flag used to be dropped in silence:
    /// '--from a --ref b --query "&lt;join&gt;"' ran as a pass-through of 'a', wrote a file missing
    /// b's columns and exited 0, while the same line misspelt '--quer' failed closed and even
    /// named --sql as the fix. Reject the right spelling in the wrong place the way the
    /// misspelling already is.
    /// </summary>
    private void RejectReaderFlagsWithoutAReader(BranchSpec branch)
    {
        if (branch.From.Count == 0 || !string.IsNullOrEmpty(branch.Input)) return;

        foreach (var token in branch.ReaderArgs)
        {
            if (!token.StartsWith('-')) continue;
            var def = _registry.Lookup(token);
            if (def == null) continue;
            if (def.Stage.HasFlag(FlagStage.Pipeline)) continue;

            throw new InvalidOperationException(
                $"Flag '{token}' configures a reader, but this branch reads from "
              + $"'{string.Join(",", branch.From)}' and has no reader of its own, so nothing binds it."
              + (def.Stage.HasFlag(FlagStage.Writer)
                    ? " It also configures a writer: move it after -o to apply it to the target."
                    : ProcessorTriggerHint()));
        }
    }

    /// <summary>
    /// Names the processors the registry actually carries, with the value each one takes.
    /// FlagRegistryFactory registers one trigger per <c>IStreamTransformerFactory.CliTriggerFlags</c>
    /// entry, so the catalogue is already here to be read; a hardcoded "use --sql" would be wrong
    /// the day a processor is added, renamed or retired, and nothing would catch it.
    /// </summary>
    private string ProcessorTriggerHint()
    {
        var triggers = _registry.GetAll()
            .Where(d => d.ProcessorTrigger)
            .Select(d => d.ConsumesNextToken ? $"{d.Name} <value>" : d.Name)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();

        return triggers.Count == 0
            ? string.Empty
            : $" A branch reading other branches is driven by a stream processor: {string.Join(", ", triggers)}.";
    }

    private void ValidateStageConstraints(string[] args, FlagStage requiredStage, string stageName)
    {
        foreach (var token in args)
        {
            if (!token.StartsWith('-')) continue;
            var def = _registry.Lookup(token);
            if (def == null) continue;
            if (!def.Stage.HasFlag(requiredStage))
                throw new InvalidOperationException(
                    $"Flag '{token}' (valid in: {def.Stage}) cannot appear {stageName}. " +
                    $"Group flags with their component: reader flags before transformers, writer flags after -o.");
        }
    }
}
