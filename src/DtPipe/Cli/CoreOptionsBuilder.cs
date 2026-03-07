using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Completions;
using System.Linq;
using DtPipe.Cli.Security;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;

namespace DtPipe.Cli;

internal static class CoreOptionsBuilder
{
    public static IReadOnlyDictionary<string, CliPipelinePhase> CoreFlagPhases { get; } = new Dictionary<string, CliPipelinePhase>
    {
        { "--input",              CliPipelinePhase.Global },
        { "--output",             CliPipelinePhase.Global },
        { "--query",              CliPipelinePhase.Reader },
        { "--alias",              CliPipelinePhase.Global },
        { "--xstreamer",          CliPipelinePhase.Global },
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
        // DAG
        { "--main",               CliPipelinePhase.XStreamer },
        { "--ref",                CliPipelinePhase.XStreamer },
        { "--src-main",           CliPipelinePhase.XStreamer },
        { "--src-ref",            CliPipelinePhase.XStreamer },
    };

    public static CoreCliOptions Build(
        IEnumerable<IStreamReaderFactory>? readerFactories = null,
        IEnumerable<IDataWriterFactory>? writerFactories = null,
        IEnumerable<IXStreamerFactory>? xstreamerFactories = null)
    {
        var inputOption = new Option<string[]>("--input")
        {
            Description = "Input connection string, file path, or '-' for stdin",
            Arity = ArgumentArity.OneOrMore
        };
        inputOption.Aliases.Add("-i");
        inputOption.CompletionSources.Add(ctx => GetInputSuggestions(ctx, readerFactories));

        var queryOption = new Option<string>("--query") { Description = "SQL query to execute (SELECT only)" };
        queryOption.Aliases.Add("-q");

        var outputOption = new Option<string[]>("--output")
        {
            Description = "Output connection string, file path, or '-' for stdout",
            Arity = ArgumentArity.OneOrMore
        };
        outputOption.Aliases.Add("-o");
        outputOption.CompletionSources.Add(ctx => GetOutputSuggestions(ctx, writerFactories));

        var connectionTimeoutOption = new Option<int>("--connection-timeout") { Description = "Connection timeout in seconds" };
        connectionTimeoutOption.DefaultValueFactory = _ => 10;

        var queryTimeoutOption = new Option<int>("--query-timeout") { Description = "Query timeout in seconds (0 = no timeout)" };
        queryTimeoutOption.DefaultValueFactory = _ => 0;

        var batchSizeOption = new Option<int>("--batch-size") { Description = "Rows per output batch" };
        batchSizeOption.DefaultValueFactory = _ => 50_000;
        batchSizeOption.Aliases.Add("-b");

        var unsafeQueryOption = new Option<bool>("--unsafe-query") { Description = "Bypass SQL validation" };
        unsafeQueryOption.DefaultValueFactory = _ => false;

        var dryRunOption = new Option<int>("--dry-run") { Description = "Dry-run mode (N rows)", Arity = ArgumentArity.ZeroOrOne };
        dryRunOption.DefaultValueFactory = _ => 0;

        var noStatsOption = new Option<bool>("--no-stats") { Description = "Disable progress bars and stats" };
        noStatsOption.DefaultValueFactory = _ => false;

        var limitOption = new Option<int>("--limit") { Description = "Max rows (0 = unlimited)" };
        limitOption.DefaultValueFactory = _ => 0;

        var samplingRateOption = new Option<double>("--sampling-rate") { Description = "Sampling probability (0.0-1.0)" };
        samplingRateOption.DefaultValueFactory = _ => 1.0;
        samplingRateOption.Aliases.Add("--sample-rate"); // Hidden alias support for backward compatibility

        var samplingSeedOption = new Option<int?>("--sampling-seed") { Description = "Seed for sampling (for reproducibility)" };
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

        var strategyOption = new Option<string>("--strategy") { Description = "Write strategy (Append, Truncate, Recreate, Upsert, Ignore)" };
        strategyOption.Aliases.Add("-s");
        strategyOption.CompletionSources.Add("Append", "Truncate", "Recreate", "Upsert", "Ignore");

        var insertModeOption = new Option<string>("--insert-mode") { Description = "Insert mode (Standard, Bulk)" };
        insertModeOption.CompletionSources.Add("Standard", "Bulk");
        var tableOption = new Option<string>("--table") { Description = "Target table name" };
        tableOption.Aliases.Add("-t");

        var strictSchemaOption = new Option<bool?>("--strict-schema") { Description = "Abort if schema errors found" };
        var noSchemaValidationOption = new Option<bool?>("--no-schema-validation") { Description = "Disable schema check" };

        var metricsPathOption = new Option<string?>("--metrics-path") { Description = "Path to structured metrics JSON output" };
        var autoMigrateOption = new Option<bool?>("--auto-migrate") { Description = "Automatically add missing columns to target table" };

        var maxRetriesOption = new Option<int>("--max-retries") { Description = "Max retries for transient errors" };
        maxRetriesOption.DefaultValueFactory = _ => 3;

        var retryDelayMsOption = new Option<int>("--retry-delay-ms") { Description = "Initial retry delay in ms" };
        retryDelayMsOption.DefaultValueFactory = _ => 1000;

        // DAG Options
        var xstreamerOption = new Option<string>("--xstreamer")
        {
            Description = "XStreamer provider (e.g. fusion-engine)",
            Arity = ArgumentArity.ZeroOrOne
        };
        xstreamerOption.Aliases.Add("-x");
        xstreamerOption.CompletionSources.Add(ctx => GetXStreamerSuggestions(ctx, xstreamerFactories));

        var aliasOption = new Option<string[]>("--alias") { Description = "Alias(es) for the current DAG branch or streams" };

        var mainOption = new Option<string>("--main") { Description = "Main source alias for XStreamer" };
        var refOption = new Option<string[]>("--ref") { Description = "Secondary source alias(es) for XStreamer" };
        var srcMainOption = new Option<string>("--src-main") { Description = "Source-main alias for DAG orchestration" };
        var srcRefOption = new Option<string[]>("--src-ref") { Description = "Source-ref alias(es) for DAG orchestration" };

        var allList = new List<Option>
        {
            inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption,
            unsafeQueryOption, dryRunOption, noStatsOption, limitOption, samplingRateOption, samplingSeedOption, keyOption,
            jobOption, exportJobOption, logOption, preExecOption, postExecOption, onErrorExecOption, finallyExecOption,
            strategyOption, insertModeOption, tableOption, maxRetriesOption, retryDelayMsOption, strictSchemaOption,
            noSchemaValidationOption, metricsPathOption, autoMigrateOption, xstreamerOption, aliasOption,
            mainOption, refOption, srcMainOption, srcRefOption
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
            strictSchemaOption, noSchemaValidationOption, metricsPathOption, autoMigrateOption, xstreamerOption,
            aliasOption, mainOption, refOption, srcMainOption, srcRefOption, allList);
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

    private static IEnumerable<CompletionItem> GetXStreamerSuggestions(CompletionContext context, IEnumerable<IXStreamerFactory>? factories)
    {
        var suggestions = new List<CompletionItem>();
        if (factories != null)
        {
            foreach (var f in factories) suggestions.Add(new CompletionItem(f.ComponentName + ":[NOSUSP]"));
        }
        return suggestions;
    }
}

public record CoreCliOptions(
    Option<string[]> Input,
    Option<string> Query,
    Option<string[]> Output,
    Option<int> ConnectionTimeout,
    Option<int> QueryTimeout,
    Option<int> BatchSize,
    Option<bool> UnsafeQuery,
    Option<int> DryRun,
    Option<bool> NoStats,
    Option<int> Limit,
    Option<double> SamplingRate,
    Option<int?> SamplingSeed,
    Option<string> Key,
    Option<string?> Job,
    Option<string?> ExportJob,
    Option<string?> Log,
    Option<string> PreExec,
    Option<string> PostExec,
    Option<string> OnErrorExec,
    Option<string> FinallyExec,
    Option<string> Strategy,
    Option<string> InsertMode,
    Option<string> Table,
    Option<int> MaxRetries,
    Option<int> RetryDelayMs,
    Option<bool?> StrictSchema,
    Option<bool?> NoSchemaValidation,
    Option<string?> MetricsPath,
    Option<bool?> AutoMigrate,
    Option<string> Xstreamer,
    Option<string[]> Alias,
    Option<string> Main,
    Option<string[]> Ref,
    Option<string> SrcMain,
    Option<string[]> SrcRef,
    List<Option> AllOptions
);
