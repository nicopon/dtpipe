using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Validation;
using Microsoft.Extensions.Logging;

namespace DtPipe.Services;

/// <summary>
/// Validates schema compatibility and performs auto-migration if enabled.
/// </summary>
public sealed class SchemaValidationService
{
    private readonly IExportObserver _observer;
    private readonly ILogger<SchemaValidationService> _logger;

    public SchemaValidationService(IExportObserver observer, ILogger<SchemaValidationService> logger)
    {
        _observer = observer;
        _logger = logger;
    }

    /// <summary>
    /// Refuses a combination in which one schema flag cancels another without a word.
    /// <c>--no-schema-validation</c> returns before the target is ever inspected, so
    /// <c>--strict-schema</c> can never fire and <c>--auto-migrate</c> never adds a column: the run
    /// does the opposite of what half the line asked for, and reports nothing.
    /// </summary>
    /// <remarks>
    /// Called from the writer-preparation step for a sample run as well, which suppresses
    /// <see cref="ValidateAndMigrateAsync"/> — a preview that proceeds where the run it previews
    /// refuses is not a preview of that run.
    /// </remarks>
    public static void RejectContradictorySettings(ISchemaValidationAware? settings)
    {
        if (settings?.NoSchemaValidation != true) return;

        var cancelled = new List<string>();
        if (settings.StrictSchema) cancelled.Add("'--strict-schema'");
        if (settings.AutoMigrate) cancelled.Add("'--auto-migrate'");
        if (cancelled.Count == 0) return;

        throw new InvalidOperationException(
            $"Flag '--no-schema-validation' contradicts {string.Join(" and ", cancelled)}: it returns "
            + $"before the target is inspected, so there is nothing left for {(cancelled.Count > 1 ? "them" : "it")} "
            + "to act on. Pass one or the other.");
    }

    /// <summary>
    /// Names the regime the run validated under. The three flags are independent and describe five
    /// states, two of which cannot be told apart from the outcome alone: a run that passes without
    /// a warning looks identical under <c>discard</c> and under <c>freeze</c>, while the guarantee
    /// is the opposite one.
    /// </summary>
    private static string DescribeMode(ISchemaValidationAware? settings) => settings switch
    {
        { NoSchemaValidation: true } =>
            "off (--no-schema-validation) - the target is not inspected",
        { AutoMigrate: true, StrictSchema: true } =>
            "evolve+freeze (--auto-migrate --strict-schema) - missing columns are added, and anything still incompatible aborts the run",
        { AutoMigrate: true } =>
            "evolve (--auto-migrate) - columns missing from the target are added",
        { StrictSchema: true } =>
            "freeze (--strict-schema) - any incompatibility aborts the run",
        _ =>
            "discard - incompatibilities are reported and the run continues; columns missing from the target are not written",
    };

    public async Task ValidateAndMigrateAsync(
        IDataWriter writer,
        IReadOnlyList<PipeColumnInfo> exportableSchema,
        ISchemaValidationAware? settings,
        CancellationToken ct)
    {
        RejectContradictorySettings(settings);

        if (writer is not ISchemaInspector inspector) return;

        if (!inspector.RequiresTargetInspection)
        {
            _logger.LogDebug("Target inspection skipped for {WriterType} (not required by current strategy).", writer.GetType().Name);
            return;
        }

        // Named before the outcome, so the line stands whether the run then warns, migrates or aborts.
        _observer.LogMessage($"[grey]Schema mode: {DescribeMode(settings)}.[/]");

        if (settings?.NoSchemaValidation == true) return;

        _observer.LogMessage("Verifying target schema compatibility...");
        var targetSchema = await inspector.InspectTargetAsync(ct);
        var dialect = (writer as IHasSqlDialect)?.Dialect;
        var report = SchemaCompatibilityAnalyzer.Analyze(exportableSchema, targetSchema, dialect);

        var missingCount = report.Columns.Count(c => c.Status == CompatibilityStatus.MissingInTarget);
        var willMigrate = missingCount > 0 && settings?.AutoMigrate == true && writer is ISchemaMigrator;

        foreach (var warning in report.Warnings) _observer.LogWarning(warning);

        // A missing column is an error only until auto-migration adds it, and its message claims
        // the data will be skipped — both wrong on a run that is about to migrate and succeed.
        // The "Auto-migrating schema" line below states what actually happens.
        foreach (var error in report.Errors)
        {
            if (willMigrate && report.MissingColumnErrors.Contains(error)) continue;
            _observer.LogError(new Exception(error));
        }

        if (report.IsCompatible)
        {
            _observer.LogMessage("Target schema compatible.");
        }

        if (willMigrate && writer is ISchemaMigrator migrator)
        {
            _observer.LogMessage($"[yellow]Auto-migrating schema: Adding {missingCount} missing columns...[/]");
            await migrator.MigrateSchemaAsync(report, ct);

            targetSchema = await inspector.InspectTargetAsync(ct);
            report = SchemaCompatibilityAnalyzer.Analyze(exportableSchema, targetSchema, dialect);

            if (!report.IsCompatible && settings?.StrictSchema == true)
                throw new InvalidOperationException("Export aborted: Schema migration failed to resolve all incompatibilities in Strict Mode.");

            _observer.LogMessage("[green]Schema migration successful. Continuing export.[/]");
        }
        else if (!report.IsCompatible && settings?.StrictSchema == true)
        {
            throw new InvalidOperationException("Export aborted due to schema incompatibilities (Strict Mode).");
        }
    }
}
