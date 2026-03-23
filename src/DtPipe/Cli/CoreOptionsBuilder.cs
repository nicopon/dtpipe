using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Completions;
using System.CommandLine.Parsing;
using System.Linq;
using DtPipe.Cli.Security;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Cli.Infrastructure;

namespace DtPipe.Cli;

internal static class CoreOptionsBuilder
{
    public static IReadOnlyDictionary<string, CliPipelinePhase> CoreFlagPhases { get; } = new Dictionary<string, CliPipelinePhase>
    {
        { "--input",              CliPipelinePhase.Global },
        { "--output",             CliPipelinePhase.Global },
        { "--query",              CliPipelinePhase.Reader },
        { "--alias",              CliPipelinePhase.Global },
        { "--sql",                CliPipelinePhase.Global },
        { "--strategy",           CliPipelinePhase.Writer },
        { "--insert-mode",        CliPipelinePhase.Writer },
        { "--table",              CliPipelinePhase.Writer },
        { "--pre-exec",           CliPipelinePhase.Writer },
        { "--post-exec",          CliPipelinePhase.Writer },
        { "--on-error-exec",      CliPipelinePhase.Writer },
        { "--finally-exec",       CliPipelinePhase.Writer },
        { "--strict-schema",      CliPipelinePhase.Writer },
        { "--no-schema-validation", CliPipelinePhase.Writer },
        { "--auto-migrate",       CliPipelinePhase.Writer },
        { "--connection-timeout", CliPipelinePhase.Reader },
        { "--query-timeout",      CliPipelinePhase.Reader },
        { "--unsafe-query",       CliPipelinePhase.Reader },
        { "--batch-size",         CliPipelinePhase.Global },
        { "--limit",              CliPipelinePhase.Global },
        { "--no-stats",           CliPipelinePhase.Global },
        { "--dry-run",            CliPipelinePhase.Global },
        { "--log",                CliPipelinePhase.Global },
        { "--key",                CliPipelinePhase.Global },
        { "--max-retries",        CliPipelinePhase.Global },
        { "--retry-delay-ms",     CliPipelinePhase.Global },
        { "--sampling-rate",      CliPipelinePhase.Global },
        { "--sampling-seed",      CliPipelinePhase.Global },
        { "--job",                CliPipelinePhase.Global },
        { "--export-job",         CliPipelinePhase.Global },
        { "--metrics-path",       CliPipelinePhase.Global },
        // Stream-transformer options
        { "--merge",              CliPipelinePhase.Transformer | CliPipelinePhase.Processor },
        { "--ref",                CliPipelinePhase.Transformer | CliPipelinePhase.Processor },
        { "--src-main",           CliPipelinePhase.Processor },
        { "--src-ref",            CliPipelinePhase.Processor },
        { "--from",               CliPipelinePhase.Global },
        { "--prefix",             CliPipelinePhase.Global },
    };

    public static CoreCliOptions Build(
        IEnumerable<IStreamReaderFactory>? readerFactories = null,
        IEnumerable<IDataWriterFactory>? writerFactories = null)
    {
        var inputOption = new Option<string[]>("--input")
        {
            Description = "Input connection string, file path, or '-' for stdin",
            Arity = ArgumentArity.OneOrMore
        };
        inputOption.Aliases.Add("-i");
        inputOption.CompletionSources.Add(ctx => GetInputSuggestions(ctx, readerFactories));

        var queryOption = new Option<string[]>("--query") { Description = "SQL query to execute (SELECT only)", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        queryOption.Aliases.Add("-q");

        var outputOption = new Option<string[]>("--output")
        {
            Description = "Output connection string (e.g., file path, db connection or '-' for stdout)",
            Arity = ArgumentArity.ZeroOrMore
        };
        outputOption.Aliases.Add("-o");
        outputOption.CompletionSources.Add(ctx => GetOutputSuggestions(ctx, writerFactories));

        var connectionTimeoutOption = new Option<int[]>("--connection-timeout") { Description = "Connection timeout in seconds", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };

        var queryTimeoutOption = new Option<int[]>("--query-timeout") { Description = "Query timeout in seconds (0 = no timeout)", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };

        var batchSizeOption = new Option<int[]>("--batch-size") { Description = "Rows per output batch", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        batchSizeOption.Aliases.Add("-b");

        var unsafeQueryOption = new Option<bool>("--unsafe-query") { Description = "Bypass SQL validation" };
        unsafeQueryOption.DefaultValueFactory = _ => false;

        var dryRunOption = new Option<int>("--dry-run") { Description = "Dry-run mode (N rows)", Arity = ArgumentArity.ZeroOrOne };
        dryRunOption.DefaultValueFactory = _ => 0;

        var noStatsOption = new Option<bool>("--no-stats") { Description = "Disable progress bars and stats" };
        noStatsOption.DefaultValueFactory = _ => false;

        var limitOption = new Option<int[]>("--limit") { Description = "Max rows (0 = unlimited)", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };

        var samplingRateOption = new Option<double[]>("--sampling-rate") { Description = "Sampling probability (0.0-1.0)", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        samplingRateOption.Aliases.Add("--sample-rate"); // Hidden alias support for backward compatibility

        var samplingSeedOption = new Option<int?[]>("--sampling-seed") { Description = "Seed for sampling (for reproducibility)", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        samplingSeedOption.Aliases.Add("--sample-seed");

        var jobOption = new Option<string?>("--job") { Description = "Path to YAML job file" };
        var exportJobOption = new Option<string?>("--export-job") { Description = "Export config to YAML" };
        var logOption = new Option<string?>("--log") { Description = "Path to log file" };
        var keyOption = new Option<string>("--key") { Description = "Primary Key columns" };

        // Lifecycle Hooks Options
        var preExecOption = new Option<string>("--pre-exec") { Description = "SQL/Command BEFORE transfer" };
        var postExecOption = new Option<string>("--post-exec") { Description = "SQL/Command AFTER transfer" };
        var onErrorExecOption = new Option<string>("--on-error-exec") { Description = "SQL/Command ON ERROR" };
        var finallyExecOption = new Option<string>("--finally-exec") { Description = "SQL/Command ALWAYS" };

        var strategyOption = new Option<string[]>("--strategy") { Description = "Write strategy (Append, Truncate, Recreate, Upsert, Ignore)", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        strategyOption.Aliases.Add("-s");
        strategyOption.CompletionSources.Add("Append", "Truncate", "Recreate", "Upsert", "Ignore");

        var insertModeOption = new Option<string[]>("--insert-mode") { Description = "Insert mode (Standard, Bulk)", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        insertModeOption.CompletionSources.Add("Standard", "Bulk");
        var tableOption = new Option<string[]>("--table") { Description = "Target table name", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        tableOption.Aliases.Add("-t");

        var strictSchemaOption = new Option<bool>("--strict-schema") { Description = "Abort if schema errors found" };
        var noSchemaValidationOption = new Option<bool>("--no-schema-validation") { Description = "Disable schema check" };

        var metricsPathOption = new Option<string[]>("--metrics-path") { Description = "Path to structured metrics JSON output", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        var autoMigrateOption = new Option<bool>("--auto-migrate") { Description = "Automatically add missing columns to target table" };

        var maxRetriesOption = new Option<int[]>("--max-retries") { Description = "Max retries for transient errors", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };

        var retryDelayMsOption = new Option<int[]>("--retry-delay-ms") { Description = "Initial retry delay in ms", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };

        // Stream Transformer Options
        var sqlOption = new Option<string[]>("--sql")
        {
            Description = "Start a SQL stream-transformer branch. Use --from for the main source, --ref for secondary (preloaded) sources.",
            Arity = ArgumentArity.ZeroOrMore,
            AllowMultipleArgumentsPerToken = true
        };

        var aliasOption = new Option<string[]>("--alias") { Description = "Alias(es) for the current DAG branch or streams" };

        var renameOption = new Option<string[]>("--rename") { Description = "Rename columns (Old:New). repeatable.", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        var dropOption = new Option<string[]>("--drop") { Description = "Drop columns. repeatable.", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        var throttleOption = new Option<int[]>("--throttle") { Description = "Throttle speed (rows/sec)", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        var ignoreNullsOption = new Option<bool>("--ignore-nulls") { Description = "Skip null values in specific transformations" };

        var mergeOption = new Option<string[]>("--merge") { Description = "Concatenate an upstream channel alias into this stream-transformer branch", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        var refOption = new Option<string[]>("--ref") { Description = "Secondary source alias(es) for SQL transformer (preloaded into memory)" };
        var srcMainOption = new Option<string[]>("--src-main") { Description = "Source-main alias for DAG orchestration", Arity = ArgumentArity.ZeroOrMore, AllowMultipleArgumentsPerToken = true };
        var srcRefOption = new Option<string[]>("--src-ref") { Description = "Source-ref alias(es) for DAG orchestration" };
        var fromOption = new Option<string[]>("--from")
        {
            Description = "Read from an upstream branch alias (fan-out / tee). Starts a new linear branch that receives a broadcast copy of the named upstream channel. Analogous to Unix 'tee'.",
            Arity = ArgumentArity.ZeroOrMore,
            AllowMultipleArgumentsPerToken = true
        };
        fromOption.CompletionSources.Add(ctx => GetAliasSuggestions(ctx));
        var prefixOption = new Option<string?>("--prefix") { Description = "Prefix for split files" };
        prefixOption.Aliases.Add("-p");

        var allList = new List<Option>
        {
            inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption,
            unsafeQueryOption, dryRunOption, noStatsOption, limitOption, samplingRateOption, samplingSeedOption, keyOption,
            jobOption, exportJobOption, logOption, preExecOption, postExecOption, onErrorExecOption, finallyExecOption,
            strategyOption, insertModeOption, tableOption, maxRetriesOption, retryDelayMsOption, strictSchemaOption,
            noSchemaValidationOption, metricsPathOption, autoMigrateOption, sqlOption, aliasOption,
            renameOption, dropOption, throttleOption, ignoreNullsOption,
            mergeOption, refOption, srcMainOption, srcRefOption, fromOption, prefixOption
        };

        foreach (var opt in allList)
        {
            if (opt.Description?.StartsWith("[HIDDEN]", StringComparison.OrdinalIgnoreCase) == true)
                opt.Hidden = true;
        }

        return new CoreCliOptions(
            inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption,
            unsafeQueryOption, dryRunOption, noStatsOption, limitOption, samplingRateOption, samplingSeedOption,
            keyOption, jobOption, exportJobOption, logOption, preExecOption, postExecOption, onErrorExecOption,
            finallyExecOption, strategyOption, insertModeOption, tableOption, maxRetriesOption, retryDelayMsOption,
            strictSchemaOption, noSchemaValidationOption, metricsPathOption, autoMigrateOption, sqlOption,
            aliasOption, renameOption, dropOption, throttleOption, ignoreNullsOption,
            mergeOption, refOption, srcMainOption, srcRefOption, fromOption, prefixOption, allList);
    }

    private static IEnumerable<CompletionItem> GetInputSuggestions(CompletionContext context, IEnumerable<IStreamReaderFactory>? factories)
    {
        var suggestions = new List<CompletionItem>();

        // We only want to suggest providers (and keyrings) if we are IMMEDIATELY after the flag.
        // E.g. "dtpipe --input [TAB]" should suggest. "dtpipe --input generate:10 [TAB]" should NOT suggest.
        bool isImmediatelyAfterFlag = false;
        try
        {
            var tokens = context.ParseResult?.Tokens.Where(t => !string.IsNullOrEmpty(t.Value)).ToList();
            if (tokens != null && tokens.Count > 0)
            {
                string wordToComplete = context.WordToComplete ?? "";
                string? relevantToken = null;

                if (string.IsNullOrEmpty(wordToComplete))
                {
                    relevantToken = tokens.Last().Value;
                }
                else if (tokens.Count > 1)
                {
                    relevantToken = tokens[tokens.Count - 2].Value;
                }

                if (relevantToken == "-i" || relevantToken == "--input") isImmediatelyAfterFlag = true;
            }
        }
        catch { /* Best effort */ }

        if (!isImmediatelyAfterFlag) return suggestions; // Empty

        if (factories != null)
        {
            foreach (var f in factories) suggestions.Add(new CompletionItem(f.ComponentName + ":[NOSUSP]"));
        }

        try
        {
            bool isTestRun = AppDomain.CurrentDomain.GetAssemblies().Any(a => a.FullName?.Contains("xunit") == true);
            if (!isTestRun && context.ParseResult != null)
            {
                string word = context.WordToComplete?.ToLowerInvariant() ?? "";
                bool looksLikeKeyring = word.StartsWith("k");

                if (looksLikeKeyring)
                {
                    var secrets = new SecretsManager().ListSecrets();
                    foreach (var alias in secrets.Keys) suggestions.Add(new CompletionItem($"keyring://{alias}"));
                }
            }
        }
        catch { /* Best effort */ }

        return suggestions;
    }

    private static IEnumerable<CompletionItem> GetOutputSuggestions(CompletionContext context, IEnumerable<IDataWriterFactory>? factories)
    {
        var suggestions = new List<CompletionItem>();

        bool isImmediatelyAfterFlag = false;
        try
        {
            var tokens = context.ParseResult?.Tokens.Where(t => !string.IsNullOrEmpty(t.Value)).ToList();
            if (tokens != null && tokens.Count > 0)
            {
                string wordToComplete = context.WordToComplete ?? "";
                string? relevantToken = null;

                if (string.IsNullOrEmpty(wordToComplete))
                {
                    relevantToken = tokens.Last().Value;
                }
                else if (tokens.Count > 1)
                {
                    relevantToken = tokens[tokens.Count - 2].Value;
                }

                if (relevantToken == "-o" || relevantToken == "--output") isImmediatelyAfterFlag = true;
            }
        }
        catch { /* Best effort */ }

        if (!isImmediatelyAfterFlag) return suggestions;

        if (factories != null)
        {
            foreach (var f in factories) suggestions.Add(new CompletionItem(f.ComponentName + ":[NOSUSP]"));
        }
        return suggestions;
    }

    private static IEnumerable<CompletionItem> GetAliasSuggestions(CompletionContext context)
    {
        // Best effort: return empty (aliases are only known at runtime, not at completion time)
        return Enumerable.Empty<CompletionItem>();
    }

}

public record CoreCliOptions(
    Option<string[]> Input,
    Option<string[]> Query,
    Option<string[]> Output,
    Option<int[]> ConnectionTimeout,
    Option<int[]> QueryTimeout,
    Option<int[]> BatchSize,
    Option<bool> UnsafeQuery,
    Option<int> DryRun,
    Option<bool> NoStats,
    Option<int[]> Limit,
    Option<double[]> SamplingRate,
    Option<int?[]> SamplingSeed,
    Option<string> Key,
    Option<string?> Job,
    Option<string?> ExportJob,
    Option<string?> Log,
    Option<string> PreExec,
    Option<string> PostExec,
    Option<string> OnErrorExec,
    Option<string> FinallyExec,
    Option<string[]> Strategy,
    Option<string[]> InsertMode,
    Option<string[]> Table,
    Option<int[]> MaxRetries,
    Option<int[]> RetryDelayMs,
    Option<bool> StrictSchema,
    Option<bool> NoSchemaValidation,
    Option<string[]> MetricsPath,
    Option<bool> AutoMigrate,
    Option<string[]> Sql,
    Option<string[]> Alias,
    Option<string[]> Rename,
    Option<string[]> Drop,
    Option<int[]> Throttle,
    Option<bool> IgnoreNulls,
    Option<string[]> Merge,
    Option<string[]> Ref,
    Option<string[]> SrcMain,
    Option<string[]> SrcRef,
    Option<string[]> From,
    Option<string?> Prefix,
    List<Option> AllOptions
);
