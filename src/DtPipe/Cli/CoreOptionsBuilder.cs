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
        { "--sampling-rate",      CliPipelinePhase.Global },
        { "--sampling-seed",      CliPipelinePhase.Global },
        { "--job",                CliPipelinePhase.Global },
        { "--export-job",         CliPipelinePhase.Global },
        { "--metrics-path",       CliPipelinePhase.Global },
        // Stream-transformer options
        { "--merge",              CliPipelinePhase.Transformer | CliPipelinePhase.Processor },
        { "--ref",                CliPipelinePhase.Transformer | CliPipelinePhase.Processor },
        { "--from",               CliPipelinePhase.Global },
        { "--prefix",             CliPipelinePhase.Global },
        // Schema persistence — Reader phase so they're per-branch in DAGs
        { "--schema-save",        CliPipelinePhase.Reader },
        { "--schema-load",        CliPipelinePhase.Reader },
        // Universal reader options — scoped to the branch's reader
        { "--path",               CliPipelinePhase.Reader },
        { "--column-types",       CliPipelinePhase.Reader },
        { "--auto-column-types",  CliPipelinePhase.Reader },
        { "--max-sample",         CliPipelinePhase.Reader },
        { "--encoding",           CliPipelinePhase.Reader },
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

        var mergeOption = new Option<bool>("--merge") { Description = "Declare a UNION ALL merge processor for the upstream channel aliases listed in --from" };
        var refOption = new Option<string[]>("--ref") { Description = "Secondary source alias(es) for SQL transformer (preloaded into memory)" };
        var fromOption = new Option<string[]>("--from")
        {
            Description = "Read from an upstream branch alias (fan-out / tee). Starts a new linear branch that receives a broadcast copy of the named upstream channel. Analogous to Unix 'tee'.",
            Arity = ArgumentArity.ZeroOrMore,
            AllowMultipleArgumentsPerToken = true
        };
        fromOption.CompletionSources.Add(ctx => GetAliasSuggestions(ctx));
        var prefixOption = new Option<string?>("--prefix") { Description = "Prefix for split files" };
        prefixOption.Aliases.Add("-p");

        var schemaSaveOption = new Option<string[]>("--schema-save")
        {
            Description = "Save discovered schema to a named .dtschema file (e.g. --schema-save erp-areas). Subsequent runs can use --schema-load to skip inference.",
            Arity = ArgumentArity.ZeroOrMore
        };
        var schemaLoadOption = new Option<string[]>("--schema-load")
        {
            Description = "Load column types from a named .dtschema file instead of running inference (e.g. --schema-load erp-areas).",
            Arity = ArgumentArity.ZeroOrMore
        };

        var pathOption = new Option<string[]>("--path")
        {
            Description = "Navigation path in the source: dot-path for JSON (e.g. 'items.data'), XPath for XML (e.g. '//Record').",
            Arity = ArgumentArity.ZeroOrMore
        };
        var columnTypesOption = new Option<string[]>("--column-types")
        {
            Description = "Explicit column types, e.g. \"Id:uuid,Count:int64,Active:bool\". Supported: uuid, string, int32, int64, double, decimal, bool, datetime, datetimeoffset.",
            Arity = ArgumentArity.ZeroOrMore
        };
        var autoColumnTypesOption = new Option<bool>("--auto-column-types")
        {
            Description = "Automatically infer and apply column types from the first sample rows (no --dry-run required)."
        };
        autoColumnTypesOption.DefaultValueFactory = _ => false;
        var maxSampleOption = new Option<int[]>("--max-sample")
        {
            Description = "Maximum rows to sample for schema inference (default: reader-defined). Arity = ZeroOrMore.",
            Arity = ArgumentArity.ZeroOrMore,
            AllowMultipleArgumentsPerToken = true
        };
        var encodingOption = new Option<string[]>("--encoding")
        {
            Description = "File encoding (e.g., UTF-8, ISO-8859-1). Defaults to UTF-8.",
            Arity = ArgumentArity.ZeroOrMore
        };

        var allList = new List<Option>
        {
            inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption,
            unsafeQueryOption, dryRunOption, noStatsOption, limitOption, samplingRateOption, samplingSeedOption, keyOption,
            jobOption, exportJobOption, logOption, preExecOption, postExecOption, onErrorExecOption, finallyExecOption,
            strategyOption, insertModeOption, tableOption, strictSchemaOption,
            noSchemaValidationOption, metricsPathOption, autoMigrateOption, sqlOption, aliasOption,
            renameOption, dropOption, throttleOption, ignoreNullsOption,
            mergeOption, refOption, fromOption, prefixOption,
            schemaSaveOption, schemaLoadOption,
            pathOption, columnTypesOption, autoColumnTypesOption, maxSampleOption, encodingOption
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
            finallyExecOption, strategyOption, insertModeOption, tableOption,
            strictSchemaOption, noSchemaValidationOption, metricsPathOption, autoMigrateOption, sqlOption,
            aliasOption, renameOption, dropOption, throttleOption, ignoreNullsOption,
            mergeOption, refOption, fromOption, prefixOption, schemaSaveOption, schemaLoadOption,
            pathOption, columnTypesOption, autoColumnTypesOption, maxSampleOption, encodingOption, allList);
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
    Option<bool> Merge,
    Option<string[]> Ref,
    Option<string[]> From,
    Option<string?> Prefix,
    Option<string[]> SchemaSave,
    Option<string[]> SchemaLoad,
    Option<string[]> Path,
    Option<string[]> ColumnTypes,
    Option<bool> AutoColumnTypes,
    Option<int[]> MaxSample,
    Option<string[]> Encoding,
    List<Option> AllOptions
);
