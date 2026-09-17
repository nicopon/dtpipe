using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;

namespace DtPipe.DryRun;

/// <summary>
/// Turns what a sample run observed into the report that is rendered or returned.
///
/// Pure derivation over the capture: it reads no source, opens no connection and runs no
/// transformer. Everything it needs already happened on the real execution path — which is
/// why this can be one definition shared by the engine and by tests, instead of each growing
/// its own version of "what the sample means".
/// </summary>
public static class SampleReportBuilder
{
    /// <param name="dialect">The target's SQL dialect, when it has one — it decides how a column
    /// name resolves to a physical name.</param>
    /// <param name="keyValidator">The target's key requirements, when it declares any.</param>
    public static SampleReport Build(
        SampleRun run,
        IReadOnlyList<string> stepNames,
        ISqlDialect? dialect,
        IKeyValidator? keyValidator,
        TargetSchemaInfo? inspectedTarget,
        string? inspectionError,
        IReadOnlyDictionary<string, string>? performanceHints = null,
        DtPipe.Sessions.ReadOnlyEnforcement enforcement = DtPipe.Sessions.ReadOnlyEnforcement.VerbScanOnly,
        string? branchAlias = null,
        string? checkpointKey = null,
        string? contractPath = null)
    {
        var finalSchema = run.FinalSchema();
        var finalRows = run.FinalRows();

        SchemaCompatibilityReport? compatibility = null;
        ConstraintValidationResult? constraintValidation = null;
        KeyValidationResult? keyValidation = null;

        if (inspectedTarget is not null)
        {
            compatibility = SchemaCompatibilityAnalyzer.Analyze(finalSchema, inspectedTarget, dialect);
            if (inspectedTarget.Exists && finalRows.Count > 0)
                constraintValidation = SampleValidationService.ValidateDataConstraints(finalRows, finalSchema, inspectedTarget, dialect);
        }

        if (keyValidator is not null)
            keyValidation = SampleValidationService.ValidatePrimaryKeys(keyValidator, finalSchema, inspectedTarget, dialect);

        return new SampleReport(
            run,
            run.ToTraces(),
            stepNames.ToList(),
            compatibility,
            inspectionError,
            dialect,
            keyValidation,
            constraintValidation,
            performanceHints,
            enforcement,
            branchAlias,
            checkpointKey,
            contractPath);
    }
}
