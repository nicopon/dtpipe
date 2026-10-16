using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Security;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Core.Security;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli;

/// <summary>
/// What a refusal tells its reader to do next, in the caller's own vocabulary: the MCP tools name the tools
/// that answer, a caller with none says nothing.
/// </summary>
/// <param name="ValidTransformerTypes">Appended to the refusal of an unknown transformer type, or empty.</param>
/// <param name="TransformerHelp">Appended to the refusal a transformer gives when it is built, by its type, or empty.</param>
public sealed record JobHints(string ValidTransformerTypes, Func<string, string> TransformerHelp)
{
    public static readonly JobHints None = new(string.Empty, _ => string.Empty);
}

/// <summary>A job text parsed, with the reasons it would not run: empty when it validates.</summary>
public sealed record ParsedJob(
    Dictionary<string, JobDefinition> Jobs,
    JobDagDefinition Dag,
    List<string> Errors);

/// <summary>
/// The one way a job text becomes something the engine runs or samples, for every caller that is not the
/// command line: parse and validate, judge the SQL it carries, sample it, resolve it for a real run. MCP and
/// <c>dtpipe studio</c> go through it, so a job validates and samples the same for both. The run itself is
/// <see cref="JobRunner"/>'s. The factories it validates against are the ones it is built with, not the
/// container's.
/// </summary>
public sealed class JobAnalysis(IServiceProvider services, IEnumerable<IStreamReaderFactory> readers, IEnumerable<IDataWriterFactory> writers)
{
    private readonly IServiceProvider _services = services;
    private readonly IEnumerable<IStreamReaderFactory> _readers = readers;
    private readonly IEnumerable<IDataWriterFactory> _writers = writers;

    /// <param name="bindInput">Alias bindings applied as <c>--bind-input</c> applies them, before validation.</param>
    /// <param name="bindOutput">Likewise for <c>--bind-output</c>.</param>
    public ParsedJob Parse(string yamlContent, string? bindInput = null, string? bindOutput = null, JobHints? hints = null)
    {
        var build = DagTopologyService.FromServices(_services).Build(yamlContent);

        // The run's own binding, at the run's own point: a bound job is checked like a complete one.
        var errors = AliasBindingApplier.Apply(build.Jobs, bindInput, bindOutput);
        var streamTransformerFactories = _services.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();
        errors.AddRange(PipelineValidator.Validate(build.Dag, build.Jobs, streamTransformerFactories));
        errors.AddRange(BranchesWithNothingToRead(build.Dag, build.Jobs, streamTransformerFactories));
        errors.AddRange(ValidateJobTransformers(build.Jobs, hints ?? JobHints.None));
        errors.AddRange(ValidateProviderOptionKeys(build.Jobs));

        return new ParsedJob(build.Jobs, build.Dag, errors);
    }

    /// <summary>
    /// A job text turned into what a real run executes: it validates, then the SQL safety policy
    /// (destructive and network access refused) judges it. The person's confirmation is the caller's,
    /// so no approval gate is asked. A job that does not validate or that the policy refuses throws,
    /// naming why, before anything runs.
    /// </summary>
    public (Dictionary<string, JobDefinition> Jobs, JobDagDefinition Dag) ResolveForRun(string yamlContent, string? bindInput, string? bindOutput)
    {
        var parsed = Parse(yamlContent, bindInput, bindOutput);
        if (parsed.Errors.Count > 0) throw new InvalidOperationException(string.Join(" ", parsed.Errors));
        var safety = CheckSafety(parsed, new SqlSafetyOptions());
        if (!safety.Allowed)
            throw new InvalidOperationException("The SQL safety policy refuses this job: " + string.Join(" ", safety.Violations)
                + " Run it with dtpipe --job and the appropriate --allow-* flag.");
        return (parsed.Jobs, parsed.Dag);
    }

    /// <summary>
    /// The SQL safety verdict on a parsed job: the SQL it carries, as the run will read it,
    /// judged by the policy the sample run uses (see <see cref="DtPipe.Sessions.JobSqlSafety"/>).
    /// </summary>
    public SqlSafetyResult CheckSafety(ParsedJob parsed, SqlSafetyOptions options) =>
        DtPipe.Sessions.JobSqlSafety.Evaluate(
            parsed.Jobs, _readers, _writers, new DefaultSqlSafetyPolicy(), options);


    /// <summary>
    /// The rule the engine applies when it resolves a reader: a branch reads from its
    /// <c>input:</c>, from another branch, from a checkpoint or a contract, or from a stream
    /// processor claiming it. None of these means the run stops before a row moves.
    ///
    /// <para>
    /// It lives on the YAML-content path only. A branch built from command-line arguments carries
    /// its input elsewhere — a channel endpoint, a resumed checkpoint — so asking the same question
    /// of <see cref="PipelineValidator"/>, which both paths share, refused pipelines that run.
    /// </para>
    ///
    /// <para>
    /// A recorded session invented a job shaped <c>job: / sources: / transformers:</c>, was told it
    /// was valid, and spent the rest of its turn re-reading a plan that could never run: a model
    /// with no error to act on has nothing to correct.
    /// </para>
    /// </summary>
    private static IEnumerable<string> BranchesWithNothingToRead(
        JobDagDefinition dag, Dictionary<string, JobDefinition> jobs, IEnumerable<IStreamTransformerFactory> processorFactories)
    {
        var factories = processorFactories.ToList();

        foreach (var branch in dag.Branches)
        {
            if (!jobs.TryGetValue(branch.Alias, out var job)) continue;
            if (!string.IsNullOrWhiteSpace(job.Input)) continue;
            if (factories.Any(f => f.IsApplicable(job))) continue;
            // A branch streaming from another reads that branch's channel, with or without a
            // processor: 'from: x' plus an 'output:' is the plain write half of a fan-out, and the
            // engine runs it. Demanding a query here refused the very shape a job needs to build
            // two related tables — one branch produces, one writes, a third joins.
            if (!string.IsNullOrWhiteSpace(job.From)) continue;
            // A checkpoint or a contract is a reader the engine resolves by capability, with no
            // 'input:' to name it.
            if (!string.IsNullOrWhiteSpace(job.FromCheckpoint) || !string.IsNullOrWhiteSpace(job.FromContract)) continue;

            yield return $"Branch '{branch.Alias}' has nothing to read: it has no 'input:', no 'from:' and "
                       + "no stream processor. Give it an 'input:', stream from another branch with "
                       + "'from:', or — when it combines several — a query under "
                       + "'provider-options -> sql -> query'.";
        }
    }



    /// <summary>
    /// The one sample path, for the <c>dry-run</c> tool and for <c>dtpipe studio</c>: validates the
    /// job, runs it in sample mode and shapes what each stage saw. Each stage shows at most
    /// <paramref name="shownRows"/> of the rows it passed: a model is given ten, a person may ask for
    /// every row read. <paramref name="bindInput"/> and <paramref name="bindOutput"/> bind the job's
    /// <c>arrow:-</c> boundaries as <c>--bind-input</c> and <c>--bind-output</c> do: no second binding path.
    /// </summary>
    public async Task<string> SampleAsync(string yamlContent, int rows, int shownRows, CancellationToken ct,
        string? bindInput = null, string? bindOutput = null, int maxRows = 1000, string? nextStep = null, JobHints? hints = null)
    {
        if (string.IsNullOrWhiteSpace(yamlContent))
            return JsonSerializer.Serialize(new { success = false, error = "YAML job content cannot be empty." });

        try
        {
            var parsed = Parse(yamlContent, bindInput, bindOutput, hints);
            if (parsed.Errors.Count > 0)
                return JsonSerializer.Serialize(new { success = false, stage = "validation", errors = parsed.Errors },
                    new JsonSerializerOptions { WriteIndented = true });

            var report = await RunSampleAsync(parsed, Math.Clamp(rows, 1, maxRows), shownRows, ct, nextStep);
            return JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new
            {
                success = false,
                stage = "dry-run",
                applied = false,
                mode = "sample",
                errors = new[] { DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message) }
            }, new JsonSerializerOptions { WriteIndented = true });
        }
    }

    /// <summary>
    /// Runs the pipeline in sample mode and shapes what it observed into JSON.
    ///
    /// It goes through JobService like any other run — the same reader, transformers,
    /// segmentation and bridges — because a tool that answered from its own walk over the data
    /// would be a second engine, and this cycle just removed the last one. Before this, the
    /// dry-run tool opened the reader and listed columns; it never ran a transformer at all,
    /// so it could not have told a model that its --window step produced nothing.
    /// </summary>
    public async Task<object> RunSampleAsync(ParsedJob parsed, int rows, int shownRows, CancellationToken ct, string? nextStep = null)
    {
        var runner = _services.GetRequiredService<JobRunner>();

        var jobs = parsed.Jobs.ToDictionary(
            kv => kv.Key,
            kv => kv.Value with { DryRunCount = rows },
            StringComparer.OrdinalIgnoreCase);

        var contexts = jobs.ToDictionary(kv => kv.Key, kv => new CliJobContext(null, null, null, System.Array.Empty<string>()));

        // Quiet: the caller is a model reading JSON, not a person watching the run. NoStats
        // alone leaves the topology panel and the results table on the console.
        var outcome = await runner.RunAsync(jobs, parsed.Dag, contexts,
            new GlobalOptions { NoStats = true, Quiet = true }, ct);
        var exitCode = outcome.ExitCode;
        string? failure = null;
        // A failed sample is still a sample that wrote nothing. Reporting it as a bare
        // error would drop `applied: false`, and a model could no longer tell "nothing was
        // written" from "something was written and then it failed" — which is exactly the
        // distinction F2 exists to keep visible.
        if (outcome.Fault is not null)
            failure = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(outcome.Fault.Message);

        // A branch that fails does not throw here: the orchestrator turns it into an exit code and
        // the reason goes to the console, which this run has silenced. Reported without them, a
        // failed sample was `success: false` with `error: null` — and validate-yaml-job now sends
        // every caller straight to this tool.
        if (failure is null && outcome.Failures.Count > 0)
            failure = string.Join(" ", outcome.Failures
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => $"Branch '{kv.Key}': {DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(kv.Value)}"));

        return new
        {
            success = exitCode == 0,
            exitCode,
            applied = false,
            mode = "sample",
            rowsRequested = rows,
            error = failure,
            // A response can be honest and still unusable. This one reports success with a full
            // trace, which a model reads as "the job is done" unless the response says what is
            // left to do — a real mission run stopped here and never wrote its target. `applied:
            // false` states the fact; nextStep states the consequence, and the model acts on the
            // second.
            nextStep,
            branches = outcome.Reports.Select(kv => Shape(kv.Key, kv.Value, shownRows)).ToList()
        };
    }

    /// <summary>Shapes one branch's report. Values become strings: a model reads them, it does not compute on them.</summary>
    private static object Shape(string alias, DtPipe.DryRun.SampleReport report, int shownRows) => new
    {
        branch = alias,
        checkpoint = report.CheckpointKey,
        rowsRead = report.Run.RowsRead,
        rowsDelivered = report.Run.RowsWritten,
        // What the run can actually support. "Nothing was written" is true of the writer; the
        // source is a separate question, and answering one with the other is how a reassuring
        // message becomes a false one.
        writerNeutralised = true,
        sourceProtection = report.Enforcement.ToString(),
        stages = report.Run.Stages.Select(s => new
        {
            index = s.Index,
            name = s.Name,
            columnar = s.IsColumnar,
            rowsSeen = s.TotalSeen,
            columns = s.Schema.Select(c => new { c.Name, type = c.ClrType?.Name ?? "unknown", c.IsNullable }).ToList(),
            sample = s.Rows.Take(shownRows).Select(r => r.Select(v => v?.ToString()).ToList()).ToList()
        }).ToList(),
        schemaCompatibility = report.CompatibilityReport is null ? null : new
        {
            errors = report.CompatibilityReport.Errors,
            warnings = report.CompatibilityReport.Warnings
        },
        schemaInspectionError = report.SchemaInspectionError is null
            ? null
            : DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(report.SchemaInspectionError),
        keyValidation = report.KeyValidation is null ? null : new
        {
            required = report.KeyValidation.IsRequired,
            valid = report.KeyValidation.IsValid,
            errors = report.KeyValidation.Errors,
            warnings = report.KeyValidation.Warnings
        },
        constraintValidation = report.ConstraintValidation is null ? null : new
        {
            errors = report.ConstraintValidation.Errors,
            warnings = report.ConstraintValidation.Warnings
        },
        performanceHints = report.PerformanceHints,
        // Reported, not judged: a produced column written over again is sometimes the point and
        // sometimes a stage doing nothing. Only the author can tell, so the fact is stated.
        producedThenReplaced = DtPipe.DryRun.SampleRunExtensions.ProducedThenReplaced(report.Run)
            .Select(r => new { column = r.Column, producedBy = r.ProducedBy, replacedBy = r.ReplacedBy })
            .ToList()
    };

    /// <summary>
    /// Checks every key under <c>provider-options</c> against the options it would bind to.
    ///
    /// <para>
    /// This is the one class of mistake an agent cannot otherwise learn from. A key that binds to
    /// nothing is a warning on the engine's stderr, which never reaches a tool result — so the run
    /// succeeds, quietly using the default, and the trace of the session shows nothing wrong. The
    /// validator is where a caller comes to be told what is wrong before running, so it is where
    /// this belongs.
    /// </para>
    ///
    /// <para>
    /// A plain block (<c>csv:</c>) may legitimately carry keys for only one side of a reader/writer
    /// pair, so it is checked against both; a suffixed block (<c>csv-writer:</c>) is checked against
    /// that side alone.
    /// </para>
    /// </summary>
    private List<string> ValidateProviderOptionKeys(Dictionary<string, JobDefinition> jobs)
    {
        var errors = new List<string>();
        // The factories this instance was built with, not the container's: they are the same set,
        // and asking the container for a collection nobody registered returns an empty one rather
        // than failing, which would silently turn every check below into a no-op.
        var factories = _readers.Cast<DtPipe.Core.Abstractions.IDataFactory>()
            .Concat(_writers).ToList();

        foreach (var (alias, job) in jobs)
        {
            if (job.ProviderOptions is null) continue;

            foreach (var (block, options) in job.ProviderOptions)
            {
                if (options is null || options.Count == 0) continue;

                var (component, writerOnly, readerOnly) = SplitBlock(block);
                var targets = factories
                    .Where(f => f.ComponentName.Equals(component, StringComparison.OrdinalIgnoreCase))
                    .Where(f => !(writerOnly && f is not DtPipe.Core.Abstractions.IDataWriterFactory)
                             && !(readerOnly && f is DtPipe.Core.Abstractions.IDataWriterFactory))
                    .ToList();

                // Silence when the block matches no registered factory. A container assembled for
                // one purpose does not register every provider, and absence of a factory is not
                // evidence of a wrong key — only a factory we found tells us what the valid keys are.
                if (targets.Count == 0) continue;

                var known = targets
                    .SelectMany(t => t.OptionsType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
                    .Where(p => p.CanWrite)
                    .Select(p => p.Name.Replace("-", string.Empty).Replace("_", string.Empty).ToLowerInvariant())
                    .ToHashSet(StringComparer.Ordinal);

                foreach (var key in options.Keys)
                {
                    if (known.Contains(key.Replace("-", string.Empty).Replace("_", string.Empty).ToLowerInvariant()))
                        continue;

                    errors.Add($"Branch '{alias}', provider-options '{block}': "
                             + OptionBinder.DescribeUnknownKey(targets[0].OptionsType, key));
                }
            }
        }

        return errors;
    }

    /// <summary>A provider-options block name split into its component and the side it pins, if any.</summary>
    private static (string Component, bool WriterOnly, bool ReaderOnly) SplitBlock(string block) =>
        block.EndsWith("-writer", StringComparison.OrdinalIgnoreCase) ? (block[..^7], true, false)
        : block.EndsWith("-reader", StringComparison.OrdinalIgnoreCase) ? (block[..^7], false, true)
        : (block, false, false);

    /// <summary>
    /// A transformer is checked by building it, which is what the engine does with the same
    /// factory and the same <see cref="YamlTransformerBuilder"/> — construction only, no reader,
    /// no connection, no row.
    ///
    /// <para>
    /// Checking the type name alone made the validator weaker than the engine on everything a
    /// transformer rejects at construction: a faker method that does not exist, a rename that is
    /// not 'Old:New', an options block that produces nothing. A recorded session was told a job
    /// was valid and delivered it; the run died on 'Unknown faker method' before a row moved.
    /// </para>
    /// </summary>
    private List<string> ValidateJobTransformers(Dictionary<string, JobDefinition> jobs, JobHints hints)
    {
        var errors = new List<string>();
        var transformerFactories = _services.GetRequiredService<IEnumerable<IDataTransformerFactory>>().ToList();

        foreach (var (alias, job) in jobs)
        {
            foreach (var t in job.Transformers ?? new List<Core.Pipelines.TransformerConfig>())
            {
                if (string.IsNullOrEmpty(t.Type)) continue;

                var factory = transformerFactories.FirstOrDefault(f => f.ComponentName.Equals(t.Type, StringComparison.OrdinalIgnoreCase));
                if (factory is null)
                {
                    errors.Add($"Branch '{alias}': Unknown transformer type '{t.Type}'.{hints.ValidTransformerTypes} Note: Joins across data sources are configured using the 'sql' provider under 'provider-options', not as a transformer type.");
                    continue;
                }

                try
                {
                    (YamlTransformerBuilder.Build(factory, t) as IDisposable)?.Dispose();
                }
                catch (Exception ex)
                {
                    errors.Add($"Branch '{alias}', transformer '{t.Type}': {DtPipe.Cli.Mcp.ToolError.Describe(ex)}{hints.TransformerHelp(t.Type)}");
                }
            }
        }

        return errors;
    }
}
