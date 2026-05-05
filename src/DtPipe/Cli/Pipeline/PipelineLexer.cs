using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Core.Pipelines;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// Sequential lexer for DtPipe pipelines.
/// Implements Strategy D: 
/// - Flags before the first '[' are global defaults.
/// - Blocks within '[' and ']' are explicit branches.
/// - Implicitly handles positional SQL as branches.
/// - Backward compatibility: implicitly splits branches on second '-i' or '--from' when not in explicit mode.
/// </summary>
public class PipelineLexer
{
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
        
        bool inExplicitBranch = false;
        bool firstExplicitBranchSeen = false;
        
        for (int i = 0; i < args.Length; i++)
        {
            var token = args[i];

            if (token == "[")
            {
                if (currentBranchArgs.Count > 0)
                {
                    branches.Add(BuildBranch(currentBranchFlags, currentBranchArgs));
                    currentBranchFlags = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                    currentBranchArgs = new List<string>();
                }
                inExplicitBranch = true;
                firstExplicitBranchSeen = true;
                continue;
            }

            if (token == "]")
            {
                if (currentBranchArgs.Count > 0)
                {
                    branches.Add(BuildBranch(currentBranchFlags, currentBranchArgs));
                    currentBranchFlags = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                    currentBranchArgs = new List<string>();
                }
                inExplicitBranch = false;
                continue;
            }

            var def = _registry.Lookup(token);
            if (def != null)
            {
                string? value = null;
                if (def.Arity != FlagArity.Boolean)
                {
                    if (i + 1 < args.Length && IsValueToken(args[i + 1]))
                    {
                        value = args[++i];
                    }
                }

                // Implicit Branching Logic (Backward Compatibility)
                if (!inExplicitBranch)
                {
                    bool alreadyHasInput = currentBranchFlags.ContainsKey("--input") || currentBranchFlags.ContainsKey("-i");
                    bool alreadyHasFrom = currentBranchFlags.ContainsKey("--from");
                    bool alreadyHasJob = currentBranchFlags.ContainsKey("--job") || currentBranchFlags.ContainsKey("-j");

                    bool isNewInput = (def.Name == "--input" || def.Name == "-i") && (alreadyHasInput || alreadyHasJob);
                    bool isNewFrom = (def.Name == "--from") && (alreadyHasFrom || alreadyHasInput || alreadyHasJob);
                    bool isNewJob = (def.Name == "--job" || def.Name == "-j") && (alreadyHasJob || alreadyHasInput);

                    if (isNewInput || isNewFrom || isNewJob)
                    {
                        branches.Add(BuildBranch(currentBranchFlags, currentBranchArgs));
                        currentBranchFlags = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                        currentBranchArgs = new List<string>();
                    }
                }

                // Global Scope handling
                if (def.Scope == FlagScope.Global || (!inExplicitBranch && !firstExplicitBranchSeen))
                {
                    globalDict[def.Name] = value ?? "true";
                    // If it's a per-branch flag seen before the first '[', it also contributes to the current branch
                    if (def.Scope == FlagScope.PerBranch)
                    {
                        if (!currentBranchFlags.ContainsKey(def.Name)) currentBranchFlags[def.Name] = new List<string>();
                        currentBranchFlags[def.Name].Add(value ?? "true");
                        
                        currentBranchArgs.Add(token);
                        if (value != null) currentBranchArgs.Add(value);
                    }
                }
                else
                {
                    // Regular branch flag
                    if (!currentBranchFlags.ContainsKey(def.Name)) currentBranchFlags[def.Name] = new List<string>();
                    currentBranchFlags[def.Name].Add(value ?? "true");
                    
                    currentBranchArgs.Add(token);
                    if (value != null) currentBranchArgs.Add(value);
                }
            }
            else
            {
                if (token.StartsWith('-'))
                {
                    // Unknown flag
                    if (!inExplicitBranch && !firstExplicitBranchSeen)
                    {
                        globalDict[token] = "true";
                    }
                    
                    if (!currentBranchFlags.ContainsKey(token)) currentBranchFlags[token] = new List<string>();
                    currentBranchFlags[token].Add("true");
                    currentBranchArgs.Add(token);
                }
                else
                {
                    // Positional token (likely SQL)
                    if (!inExplicitBranch && !firstExplicitBranchSeen && currentBranchArgs.Count > 0 && !currentBranchFlags.ContainsKey("--from"))
                    {
                        // Split implicit reader from positional SQL
                        branches.Add(BuildBranch(currentBranchFlags, currentBranchArgs));
                        currentBranchFlags = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                        currentBranchArgs = new List<string>();
                    }

                    if (!currentBranchFlags.ContainsKey("--sql")) currentBranchFlags["--sql"] = new List<string>();
                    currentBranchFlags["--sql"].Add(token);
                    
                    currentBranchArgs.Add("--sql");
                    currentBranchArgs.Add(token);
                }
            }
        }

        if (currentBranchArgs.Count > 0)
        {
            branches.Add(BuildBranch(currentBranchFlags, currentBranchArgs));
        }

        return new ParsedPipeline(MapGlobals(globalDict), branches);
    }

    // A token is a flag value if it doesn't start with '-', OR if it looks like a negative number.
    private static bool IsValueToken(string token)
    {
        if (token == "[" || token == "]") return false;
        if (!token.StartsWith('-')) return true;
        return token.Length > 1 && (char.IsDigit(token[1]) || (token[1] == '.' && token.Length > 2 && char.IsDigit(token[2])));
    }

    private GlobalOptions MapGlobals(Dictionary<string, object?> dict)
    {
        T? Get<T>(string key, T? @default = default)
        {
            if (!dict.TryGetValue(key, out var val)) return @default;
            try {
                if (typeof(T) == typeof(int)) return (T)(object)int.Parse(val?.ToString() ?? "0");
                if (typeof(T) == typeof(double)) return (T)(object)double.Parse(val?.ToString() ?? "0", System.Globalization.CultureInfo.InvariantCulture);
                if (typeof(T) == typeof(bool)) return (T)(object)true;
                return (T?)val;
            } catch { return @default; }
        }

        return new GlobalOptions
        {
            BatchSize = Get<int>("--batch-size", 50_000),
            Limit = Get<int>("--limit", 0),
            Key = Get<string>("--key") ?? Get<string>("-k"),
            SamplingRate = Get<double>("--sampling-rate", 1.0),
            SamplingSeed = dict.ContainsKey("--sampling-seed") ? Get<int>("--sampling-seed") : (int?)null,
            NoStats = dict.ContainsKey("--no-stats"),
            DryRunCount = GetDryRun(dict),
            JobFile = Get<string>("--job") ?? Get<string>("-j"),
            ExportJobFile = Get<string>("--export-job"),
            LogPath = Get<string>("--log"),
            MetricsPath = Get<string>("--metrics-path"),
            Prefix = Get<string>("--prefix"),
            AllFlags = dict
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
            {
                if (flags.TryGetValue(k, out var list)) return list.LastOrDefault();
            }
            return null;
        }

        string[] GetList(params string[] keys)
        {
            var result = new List<string>();
            foreach (var k in keys)
            {
                if (flags.TryGetValue(k, out var list)) result.AddRange(list);
            }
            return result.ToArray();
        }

        return new BranchSpec
        {
            Input = GetSingle("--input", "-i"),
            Output = GetSingle("--output", "-o"),
            Query = GetSingle("--query", "-q"),
            Alias = GetSingle("--alias"),
            From = GetList("--from").SelectMany(s => s.Split(',')).Select(s => s.Trim()).ToList(),
            Ref = GetList("--ref").SelectMany(s => s.Split(',')).Select(s => s.Trim()).ToList(),
            Strategy = GetSingle("--strategy"),
            InsertMode = GetSingle("--insert-mode"),
            Table = GetSingle("--table", "-t"),
            BatchSize = int.TryParse(GetSingle("--batch-size"), out var bs) ? bs : 0,
            Limit = int.TryParse(GetSingle("--limit"), out var lim) ? lim : 0,
            LogPath = GetSingle("--log"),
            MetricsPath = GetSingle("--metrics-path"),

            // Per-branch execution / reader options
            Path = GetSingle("--path"),
            ColumnTypes = GetSingle("--column-types"),
            AutoColumnTypes = flags.ContainsKey("--auto-column-types"),
            MaxSample = int.TryParse(GetSingle("--max-sample"), out var mxs) ? mxs : 0,
            Encoding = GetSingle("--encoding"),
            ConnectionTimeout = int.TryParse(GetSingle("--connection-timeout"), out var cto) ? cto : 0,
            QueryTimeout = int.TryParse(GetSingle("--query-timeout"), out var qto) ? qto : 0,
            UnsafeQuery = flags.ContainsKey("--unsafe-query"),
            StrictSchema = flags.ContainsKey("--strict-schema"),
            NoSchemaValidation = flags.ContainsKey("--no-schema-validation"),
            AutoMigrate = flags.ContainsKey("--auto-migrate"),
            PreExec = GetSingle("--pre-exec"),
            PostExec = GetSingle("--post-exec"),
            OnErrorExec = GetSingle("--on-error-exec"),
            FinallyExec = GetSingle("--finally-exec"),
            Key = GetSingle("--key", "-k"),
            SamplingRate = double.TryParse(GetSingle("--sampling-rate"), System.Globalization.CultureInfo.InvariantCulture, out var sr) ? sr : 1.0,
            SamplingSeed = int.TryParse(GetSingle("--sampling-seed"), out var ss) ? (int?)ss : null,
            Prefix = GetSingle("--prefix", "-p"),
            SchemaSave = GetSingle("--schema-save"),
            SchemaLoad = GetSingle("--schema-load"),

            RawArgs = rawArgs.ToArray(),
            Flags = flags
        };
    }
}
