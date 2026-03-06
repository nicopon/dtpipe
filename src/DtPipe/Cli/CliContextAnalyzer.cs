using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using DtPipe.Cli.Validation;

namespace DtPipe.Cli;

public static class CliContextAnalyzer
{
    public static CliCompletionContext Analyze(string[] tokensBeforeCursor, IReadOnlyList<Option> knownOptions)
    {
        var currentFlags = new HashSet<string>();
        var currentAliases = new List<string>();
        var allPreviousAliases = new List<string>();
        bool isXStreamer = false;
        bool hasSeenSourceInCurrentBranch = false;
        string? lastFlag = null;
        bool lastFlagExpectsValue = false;
        string? currentInputPrefix = null;
        bool hasTerminator = false;
        int branchIndex = 0;

        // Map options and aliases to their long name and max arity
        var optionMapper = new Dictionary<string, (string LongName, int MaxArity)>(StringComparer.OrdinalIgnoreCase);
        foreach (var opt in knownOptions)
        {
            var entry = (opt.Name, opt.Arity.MaximumNumberOfValues);
            optionMapper[opt.Name] = entry;
            foreach (var alias in opt.Aliases)
            {
                optionMapper[alias] = entry;
            }
        }

        foreach (var token in tokensBeforeCursor)
        {
            bool isPrimarySource = CliPipelineRules.PrimarySourceFlags.Contains(token);

            if (isPrimarySource && hasSeenSourceInCurrentBranch)
            {
                // Finalize branch and start new one
                allPreviousAliases.AddRange(currentAliases);
                branchIndex++;

                currentFlags.Clear();
                currentAliases.Clear();
                lastFlag = null;
                lastFlagExpectsValue = false;
                currentInputPrefix = null;
                hasTerminator = false;

                // Determine if this new branch starts as XStreamer
                isXStreamer = CliPipelineRules.XStreamerFlags.Contains(token);
                hasSeenSourceInCurrentBranch = true;
            }

            if (token.StartsWith("-"))
            {
                if (optionMapper.TryGetValue(token, out var match))
                {
                    string longName = match.LongName;
                    currentFlags.Add(longName);

                    if (CliPipelineRules.SourceFlags.Contains(longName))
                        hasSeenSourceInCurrentBranch = true;

                    if (CliPipelineRules.IsTerminator(longName))
                        hasTerminator = true;

                    if (longName == "--xstreamer") isXStreamer = true;

                    lastFlag = longName;
                    lastFlagExpectsValue = match.MaxArity > 0;
                }
                else if (token.Equals("--main", StringComparison.OrdinalIgnoreCase) || token.Equals("--ref", StringComparison.OrdinalIgnoreCase))
                {
                    // Special intrinsic DAG flags (Sources)
                    currentFlags.Add(token.ToLowerInvariant());
                    hasSeenSourceInCurrentBranch = true;
                    lastFlag = token.ToLowerInvariant();
                    lastFlagExpectsValue = true;
                }
                else
                {
                    // Unknown flag, treat as not expecting value to be safe
                    lastFlag = token;
                    lastFlagExpectsValue = false;
                }
            }
            else
            {
                // It's a value
                if (lastFlag == "--alias")
                {
                    currentAliases.Add(token);
                }
                else if (lastFlag == "--input")
                {
                    currentInputPrefix = ExtractPrefix(token);
                }
                lastFlagExpectsValue = false;
                lastFlag = null;
            }
        }

        return new CliCompletionContext
        {
            CurrentBranchIndex = branchIndex,
            IsXStreamerBranch = isXStreamer,
            LastCompletedFlag = lastFlagExpectsValue ? lastFlag : null,
            UsedFlagsInCurrentBranch = new HashSet<string>(currentFlags),
            KnownAliases = allPreviousAliases.ToArray(),
            CurrentInputPrefix = currentInputPrefix,
            IsExpectingFlagValue = lastFlagExpectsValue,
            HasOutput = hasTerminator,
            AllSourceFlags = knownOptions
                .Where(o => CliPipelineRules.SourceFlags.Contains(o.Name))
                .Select(o => o.Name)
                .ToList()
        };
    }

    private static string? ExtractPrefix(string value)
    {
        int colonIndex = value.IndexOf(':');
        return colonIndex >= 0 ? value.Substring(0, colonIndex + 1) : null;
    }
}
