using System.Collections.Generic;
using System.CommandLine;

namespace DtPipe.Cli;

internal static class CoreOptionsBuilder
{
    public static CoreCliOptions Build()
    {
        var inputOption = new Option<string?>("--input") { Description = "Input connection string, file path, or '-' for stdin" };
        inputOption.Aliases.Add("-i");

        var queryOption = new Option<string?>("--query") { Description = "SQL query to execute (SELECT only)" };
        queryOption.Aliases.Add("-q");

        var outputOption = new Option<string?>("--output") { Description = "Output connection string, file path, or '-' for stdout" };
        outputOption.Aliases.Add("-o");

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
        var keyOption = new Option<string?>("--key") { Description = "Primary Key columns" };

        // Lifecycle Hooks Options
        var preExecOption = new Option<string?>("--pre-exec") { Description = "SQL/Command BEFORE transfer" };
        var postExecOption = new Option<string?>("--post-exec") { Description = "SQL/Command AFTER transfer" };
        var onErrorExecOption = new Option<string?>("--on-error-exec") { Description = "SQL/Command ON ERROR" };
        var finallyExecOption = new Option<string?>("--finally-exec") { Description = "SQL/Command ALWAYS" };

        var strategyOption = new Option<string?>("--strategy") { Description = "Write strategy (Append, Truncate, Recreate, Upsert, Ignore)" };
        strategyOption.Aliases.Add("-s");

        var insertModeOption = new Option<string?>("--insert-mode") { Description = "Insert mode (Standard, Bulk)" };
        var tableOption = new Option<string?>("--table") { Description = "Target table name" };
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
        var xstreamerOption = new Option<string[]>("--xstreamer") { Description = "XStreamer provider (e.g. duck)" };
        xstreamerOption.Aliases.Add("-x");

        var aliasOption = new Option<string[]>("--alias") { Description = "Alias(es) for the current DAG branch or streams" };

        var allList = new List<Option>
        {
            inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption,
            unsafeQueryOption, dryRunOption, noStatsOption, limitOption, samplingRateOption, samplingSeedOption, keyOption,
            jobOption, exportJobOption, logOption, preExecOption, postExecOption, onErrorExecOption, finallyExecOption,
            strategyOption, insertModeOption, tableOption, maxRetriesOption, retryDelayMsOption, strictSchemaOption,
            noSchemaValidationOption, metricsPathOption, autoMigrateOption, xstreamerOption, aliasOption
        };

        return new CoreCliOptions(
            inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption,
            unsafeQueryOption, dryRunOption, noStatsOption, limitOption, samplingRateOption, samplingSeedOption,
            keyOption, jobOption, exportJobOption, logOption, preExecOption, postExecOption, onErrorExecOption,
            finallyExecOption, strategyOption, insertModeOption, tableOption, maxRetriesOption, retryDelayMsOption,
            strictSchemaOption, noSchemaValidationOption, metricsPathOption, autoMigrateOption, xstreamerOption,
            aliasOption, allList);
    }
}

internal record CoreCliOptions(
    Option<string?> Input,
    Option<string?> Query,
    Option<string?> Output,
    Option<int> ConnectionTimeout,
    Option<int> QueryTimeout,
    Option<int> BatchSize,
    Option<bool> UnsafeQuery,
    Option<int> DryRun,
    Option<bool> NoStats,
    Option<int> Limit,
    Option<double> SamplingRate,
    Option<int?> SamplingSeed,
    Option<string?> Key,
    Option<string?> Job,
    Option<string?> ExportJob,
    Option<string?> Log,
    Option<string?> PreExec,
    Option<string?> PostExec,
    Option<string?> OnErrorExec,
    Option<string?> FinallyExec,
    Option<string?> Strategy,
    Option<string?> InsertMode,
    Option<string?> Table,
    Option<int> MaxRetries,
    Option<int> RetryDelayMs,
    Option<bool?> StrictSchema,
    Option<bool?> NoSchemaValidation,
    Option<string?> MetricsPath,
    Option<bool?> AutoMigrate,
    Option<string[]> Xstreamer,
    Option<string[]> Alias,
    List<Option> AllOptions
);
