using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using DtPipe.Cli.Validation;

namespace DtPipe.Cli;

public static class ContextualCompletionProvider
{
    public static IEnumerable<string> GetCompletions(
        RootCommand rootCommand,
        string[] rawWords,
        int cursorPos,
        IReadOnlyList<Option> allOptions)
    {
        try
        {
            // 1. Extract tokensBeforeCursor
            string[] tokensBeforeCursor = Array.Empty<string>();
            if (cursorPos > 1)
            {
                tokensBeforeCursor = rawWords.Skip(1).Take(cursorPos - 1).ToArray();
            }

            // 2. Analyze context
            var context = CliContextAnalyzer.Analyze(tokensBeforeCursor, allOptions);

            // 3. Get base completions
            var suggestPR = rootCommand.Parse(rawWords);
            var baseCompletions = suggestPR.GetCompletions(cursorPos).Select(c => c.Label).ToList();

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

            // --- Pipeline-Specific Logic (RootCommand only) ---
            var baseFiltered = generalFiltered;

            // 4. Force-inject root options if the user is typing a new flag or at a space.
            string currentWord = cursorPos < rawWords.Length ? rawWords[cursorPos] : "";
            if (string.IsNullOrEmpty(currentWord) || currentWord.StartsWith("-"))
            {
                var inject = allOptions.Select(o => o.Name).Where(name => !name.StartsWith("-") || name.StartsWith("--"));
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

            return ApplyLinearGuide(pipelineFiltered, context);
        }
        catch (Exception)
        {
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
}
