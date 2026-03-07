using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using System.Linq;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Validation;

namespace DtPipe.Cli;

public static class ContextualCompletionProvider
{
    public static IEnumerable<string> GetCompletions(
        RootCommand rootCommand,
        string[] rawWords,
        int cursorPos,
        IReadOnlyList<Option> allOptions,
        IReadOnlyDictionary<string, CliPipelinePhase> flagPhases,
        IReadOnlyList<ICliContributor> contributors)
    {
        try
        {
            // 1. Extract tokensBeforeCursor
            string[] tokensBeforeCursor = Array.Empty<string>();
            if (cursorPos > 0)
            {
                tokensBeforeCursor = rawWords.Take(cursorPos).ToArray();
            }

            // 2. Analyze context
            var context = CliContextAnalyzer.Analyze(tokensBeforeCursor, allOptions);

            if (Environment.GetEnvironmentVariable("DEBUG") == "1")
            {
                Console.Error.WriteLine($"[DEBUG-CORE] Analyzer CurrentBranchIndex: {context.CurrentBranchIndex}, LastCompletedFlag: {context.LastCompletedFlag}");
                Console.Error.WriteLine($"[DEBUG-CORE] UsedFlagsInCurrentBranch: {string.Join(", ", context.UsedFlagsInCurrentBranch)}");
            }

            // 3. Calculate Text Offset for System.CommandLine
            int textOffset = 0;
            if (cursorPos < rawWords.Length)
            {
                textOffset = string.Join(" ", rawWords.Take(cursorPos + 1)).Length;
            }
            else
            {
                textOffset = string.Join(" ", rawWords).Length;
            }

            var suggestPR = rootCommand.Parse(rawWords);
            if (Environment.GetEnvironmentVariable("DEBUG") == "1")
            {
                var inputOpt = allOptions.FirstOrDefault(o => o.Name == "input" || o.Aliases.Contains("--input"));
                if (inputOpt == null) Console.Error.WriteLine("[DEBUG-COMPLETION-TOKENS] --input option not found in allOptions!");
                else Console.Error.WriteLine($"[DEBUG-COMPLETION-TOKENS] --input option found. Has default completions? (can't easily check without reflection, but we found it: {inputOpt.Name})");

                if (suggestPR.Errors.Any())
                {
                    Console.Error.WriteLine($"[DEBUG-COMPLETION-PR-ERROR] {suggestPR.Errors.Count} errors found:");
                    foreach(var e in suggestPR.Errors) Console.Error.WriteLine(e.Message);
                }
                Console.Error.WriteLine($"[DEBUG-COMPLETION-TOKENS] Parsed token count: {suggestPR.Tokens.Count}");
                foreach(var t in suggestPR.Tokens)
                {
                    Console.Error.WriteLine($"  - {t.Value} ({t.Type})");
                }
            }
            var baseCompletions = suggestPR.GetCompletions(textOffset).Select(c => c.Label).ToList();

            string currentWord = cursorPos < rawWords.Length ? rawWords[cursorPos] : "";
            string prevWord = cursorPos > 0 ? rawWords[cursorPos - 1] : "";

            bool isAfterValueRequiringFlag = prevWord == "-i" || prevWord == "--input" ||
                                             prevWord == "-o" || prevWord == "--output" ||
                                             prevWord == "-s" || prevWord == "--strategy" ||
                                             prevWord == "--alias" || prevWord == "-t" || prevWord == "--table";

            if (isAfterValueRequiringFlag)
            {
                // We are completing a value for a specific option.
                // System.CommandLine's text-offset lexer is notoriously bugged when options have Arity=ZeroOrMore
                // earlier in the pipeline (it aggressively swallows trailing text thinking it's still their argument).
                // To guarantee we trigger the correct Option's CompletionSources,
                // we parse a minimalist command and ask for its completions perfectly isolated.

                var minimalWords = new string[] { prevWord, currentWord };
                var minimalOffset = string.Join(" ", minimalWords).Length;

                var minimalPR = rootCommand.Parse(minimalWords);
                var targetedCompletions = minimalPR.GetCompletions(minimalOffset).Select(c => c.Label).ToList();

                return targetedCompletions;
            }

            // 3.1. General Junk Filtering (decluttering for subcommands and root)
            bool isFirstArg = tokensBeforeCursor.Length == 0;
            var generalFiltered = baseCompletions.Where(label =>
            {
                // Hide short aliases (e.g. -i, -o, -h, -?) to declutter suggestions as per user request
                // Autocompletion should be explicit; long versions are preferred.
                if (label.StartsWith("-") && !label.StartsWith("--")) return false;
                if (label == "/?" || label == "/h") return false;

                // Hide help tools and subcommands gracefully if we are already inside a command or building a pipeline
                if (!isFirstArg && (label == "--help" || label == "--version" ||
                                    label == "inspect" || label == "providers" ||
                                    label == "secret" || label == "completion")) return false;

                return true;
            }).ToList();

            // 3.2. If it's a subcommand, return general filtered results immediately
            if (suggestPR.CommandResult.Command != rootCommand)
            {
                return generalFiltered;
            }

            // 3.3. Phase Filtering
            var phaseFiltered = generalFiltered.Where(label => IsVisibleInPhase(label, context, flagPhases, contributors)).ToList();

            // --- Pipeline-Specific Logic (RootCommand only) ---
            var baseFiltered = phaseFiltered;

            // 4. Force-inject root options if the user is typing a new flag or at a space.
            if (string.IsNullOrEmpty(currentWord) || currentWord.StartsWith("-"))
            {
                var inject = allOptions
                    .Select(o => o.Aliases.FirstOrDefault(a => a.StartsWith("--")) ?? (o.Name.StartsWith("-") ? o.Name : $"--{o.Name}"))
                    .Where(label => IsVisibleInPhase(label, context, flagPhases, contributors))
                    .ToList();

                baseFiltered.AddRange(inject);
                baseFiltered = baseFiltered.Distinct().ToList();
            }

            // 5. DAG-aware rule
            if (context.LastCompletedFlag == "--main" || context.LastCompletedFlag == "--ref")
            {
                if (context.KnownAliases.Any())
                {
                    return context.KnownAliases;
                }
                return Enumerable.Empty<string>();
            }

            // 6. Deduplication of SINGLETON flags
            var blockedLabels = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var usedFlag in context.UsedFlagsInCurrentBranch)
            {
                if (!CliPipelineRules.IsSingleton(usedFlag)) continue;

                var opt = allOptions.FirstOrDefault(o =>
                    o.Name.Equals(usedFlag, StringComparison.OrdinalIgnoreCase) ||
                    o.Aliases.Contains(usedFlag, StringComparer.OrdinalIgnoreCase));

                if (opt != null)
                {
                    blockedLabels.Add(opt.Name);
                    foreach (var alias in opt.Aliases)
                    {
                        blockedLabels.Add(alias);
                    }
                }
            }

            var pipelineFiltered = baseFiltered.Where(label => !blockedLabels.Contains(label));

            var result = ApplyLinearGuide(pipelineFiltered, context).ToList();
            return result;
        }
        catch (Exception ex)
        {
            if (Environment.GetEnvironmentVariable("DEBUG") == "1")
            {
                Console.Error.WriteLine($"[DEBUG-COMPLETION-ERROR] {ex}");
            }
            return Enumerable.Empty<string>();
        }
    }

    private static IEnumerable<string> ApplyLinearGuide(IEnumerable<string> suggestions, CliCompletionContext context)
    {
        // 1. Detect Phase
        bool hasSource = context.UsedFlagsInCurrentBranch.Any(f =>
            CliPipelineRules.SourceFlags.Contains(f));

        bool hasTerminator = context.HasOutput; // context.HasOutput tracks IsTerminator(f) from analyzer

        // Phase 0 (Start): No source (Input/XStreamer/Main/Ref) defined yet
        if (!hasSource)
        {
            return suggestions.Where(s => CliPipelineRules.StartRules.Contains(s));
        }

        // Phase 2 (Output & Global): Output or Alias has been defined
        if (hasTerminator)
        {
            // We specially allow SourceFlags even if they were already used in this branch,
            // because they logically signify the start of a NEW branch when used in Phase 2.
            var phase2Suggestions = suggestions.Where(s =>
                CliPipelineRules.OutputRules.Contains(s) ||
                CliPipelineRules.GlobalRules.Contains(s)).ToList();

            // Add all possible source flags from baseCompletions (re-injecting them if they were filtered by singleton)
            var sourceSuggestions = context.AllSourceFlags.Where(s => CliPipelineRules.SourceFlags.Contains(s));

            return phase2Suggestions.Concat(sourceSuggestions).Distinct();
        }

        // Phase 1 (Input & Transformers): Input/Source defined, building pipeline
        var filtered = suggestions.Where(s =>
        {
            // Allowed: Input-specific options and Sinks (Output/Alias)
            if (CliPipelineRules.InputRules.Contains(s)) return true;
            if (CliPipelineRules.IsTerminator(s)) return true;

            // Allowed: Repeating Source flags for DAG branch chaining
            if (CliPipelineRules.SourceFlags.Contains(s)) return true;

            // Allowed: Transformers (anything not defined in core rule sets)
            bool isCore = CliPipelineRules.StartRules.Contains(s) ||
                          CliPipelineRules.InputRules.Contains(s) ||
                          CliPipelineRules.OutputRules.Contains(s) ||
                          CliPipelineRules.GlobalRules.Contains(s);

            return !isCore;
        }).ToList();

        // Priority logic
        var highPriority = new List<string>();

        // If we are in an XStreamer branch, we MUST provide sources (--main, --ref)
        if (context.IsXStreamerBranch)
        {
            highPriority.Add("--main");
            highPriority.Add("--ref");
        }

        // Guide towards completion
        highPriority.Add("--output");
        highPriority.Add("--alias");

        var prioritized = filtered.Where(s => highPriority.Contains(s, StringComparer.OrdinalIgnoreCase)).ToList();
        var rest = filtered.Where(s => !highPriority.Contains(s, StringComparer.OrdinalIgnoreCase)).ToList();

        return prioritized.Concat(rest);
    }

    private static bool IsVisibleInPhase(
        string label,
        CliCompletionContext context,
        IReadOnlyDictionary<string, CliPipelinePhase> flagPhases,
        IReadOnlyList<ICliContributor> contributors)
    {
        // Evaluate contextual dependencies
        foreach (var c in contributors)
        {
            if (c.FlagDependencies.TryGetValue(label, out var requiredFlag))
            {
                if (!context.UsedFlagsInCurrentBranch.Contains(requiredFlag))
                {
                    return false;
                }
            }
        }

        if (!flagPhases.TryGetValue(label, out var phase))
        {
            return true; // Unknown flag: show by default (safe fallback)
        }

        return phase switch
        {
            CliPipelinePhase.Global => true,

            CliPipelinePhase.Reader =>
                // Show global Reader options (like --query) or component-specific Reader options
                EvaluateReaderPhase(label, flagPhases, contributors, context),

            CliPipelinePhase.Transformer =>
                // Transformers visible only in Phase 1 (source defined, no output yet)
                context.ActivePhase == CliPipelinePhase.Transformer,

            CliPipelinePhase.Writer =>
                // Writers visible only after --output is defined
                context.HasOutput || context.ActivePhase == CliPipelinePhase.Writer,

            CliPipelinePhase.XStreamer =>
                // XStreamer options visible only in XStreamer branches
                context.IsXStreamerBranch,

            _ => true
        };
    }

    private static bool EvaluateReaderPhase(string label, IReadOnlyDictionary<string, CliPipelinePhase> flagPhases, IReadOnlyList<ICliContributor> contributors, CliCompletionContext context)
    {
        bool isCoreReaderOption = Validation.CliPipelineRules.InputRules.Contains(label);
        bool isComponent = contributors.Any(c => c.BoundComponentName != null && context.CurrentInputPrefix == c.BoundComponentName + ":" && c.FlagPhases.ContainsKey(label));
        return isCoreReaderOption || isComponent;
    }
}
