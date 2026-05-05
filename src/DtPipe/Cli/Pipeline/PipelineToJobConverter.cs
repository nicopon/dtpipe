using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Pipeline;

public static class PipelineToJobConverter
{
    public static (Dictionary<string, JobDefinition> Jobs, JobDagDefinition Dag) Convert(
        ParsedPipeline parsed,
        IEnumerable<IStreamTransformerFactory>? streamTransformerFactories = null)
    {
        // --job mode: load from YAML file and apply CLI overrides
        if (!string.IsNullOrEmpty(parsed.Globals.JobFile))
            return ConvertFromJobFile(parsed);

        var jobs = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase);
        var branches = new List<BranchDefinition>();

        // Pass 1: Collect explicit aliases to avoid collisions
        var explicitAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var b in parsed.Branches)
        {
            if (!string.IsNullOrEmpty(b.Alias))
                explicitAliases.Add(b.Alias);
        }

        var processorFactories = streamTransformerFactories?.ToList();

        int branchCounter = 1;
        foreach (var branchSpec in parsed.Branches)
        {
            var alias = branchSpec.Alias;
            if (string.IsNullOrEmpty(alias))
            {
                while (explicitAliases.Contains($"stream{branchCounter}"))
                    branchCounter++;
                alias = $"stream{branchCounter}";
                branchCounter++;
            }

            var job = MapToJobDefinition(parsed.Globals, branchSpec);

            var processorName = processorFactories?
                .FirstOrDefault(f => f.IsApplicable(branchSpec.RawArgs))
                ?.ComponentName;

            jobs[alias] = job;
            branches.Add(new BranchDefinition
            {
                Alias = alias,
                Input = job.Input,
                Output = job.Output,
                StreamingAliases = branchSpec.From.ToArray(),
                RefAliases = branchSpec.Ref.ToArray(),
                Arguments = branchSpec.RawArgs,
                ProcessorName = processorName
            });
        }

        var dag = new JobDagDefinition { Branches = branches };
        return (jobs, dag);
    }

    private static (Dictionary<string, JobDefinition> Jobs, JobDagDefinition Dag) ConvertFromJobFile(ParsedPipeline parsed)
    {
        var jobs = JobFileParser.Parse(parsed.Globals.JobFile!);

        // Apply CLI overrides to all loaded jobs
        foreach (var alias in jobs.Keys.ToList())
        {
            var job = jobs[alias];
            if (parsed.Globals.DryRunCount > 0) job = job with { DryRunCount = parsed.Globals.DryRunCount };
            if (parsed.Globals.Limit > 0) job = job with { Limit = parsed.Globals.Limit };
            if (parsed.Globals.BatchSize > 0) job = job with { BatchSize = parsed.Globals.BatchSize };
            if (!string.IsNullOrEmpty(parsed.Globals.Key)) job = job with { Key = parsed.Globals.Key };
            if (!string.IsNullOrEmpty(parsed.Globals.LogPath)) job = job with { LogPath = parsed.Globals.LogPath };
            if (!string.IsNullOrEmpty(parsed.Globals.MetricsPath)) job = job with { MetricsPath = parsed.Globals.MetricsPath };
            if (!string.IsNullOrEmpty(parsed.Globals.ExportJobFile)) job = job with { Arguments = Array.Empty<string>() };
            jobs[alias] = job;
        }

        var branches = jobs.Select(kv => new BranchDefinition
        {
            Alias = kv.Key,
            Input = kv.Value.Input,
            Output = kv.Value.Output,
            StreamingAliases = kv.Value.From != null ? new[] { kv.Value.From } : Array.Empty<string>(),
            RefAliases = kv.Value.Ref ?? Array.Empty<string>(),
            Arguments = Array.Empty<string>(),
            ProcessorName = kv.Value.Sql != null ? "sql" : null
        }).ToList();

        return (jobs, new JobDagDefinition { Branches = branches });
    }

    private static JobDefinition MapToJobDefinition(GlobalOptions globals, BranchSpec branch)
    {
        return new JobDefinition
        {
            Input = branch.Input,
            Output = branch.Output,
            Query = branch.Query,
            Table = branch.Table,
            BatchSize = branch.BatchSize != 0 ? branch.BatchSize : (globals.BatchSize != 0 ? globals.BatchSize : 50000),
            DryRunCount = globals.DryRunCount,
            Sql = null,
            Strategy = branch.Strategy,
            InsertMode = branch.InsertMode,
            Limit = branch.Limit != 0 ? branch.Limit : globals.Limit,
            Key = branch.Key ?? globals.Key,
            SamplingRate = branch.SamplingRate != 1.0 ? branch.SamplingRate : globals.SamplingRate,
            SamplingSeed = branch.SamplingSeed ?? globals.SamplingSeed,
            LogPath = branch.LogPath ?? globals.LogPath,
            MetricsPath = branch.MetricsPath ?? globals.MetricsPath,
            Prefix = branch.Prefix ?? globals.Prefix,

            Path = branch.Path,
            ColumnTypes = branch.ColumnTypes,
            AutoColumnTypes = branch.AutoColumnTypes,
            MaxSample = branch.MaxSample,
            Encoding = branch.Encoding,
            ConnectionTimeout = branch.ConnectionTimeout,
            QueryTimeout = branch.QueryTimeout,
            UnsafeQuery = branch.UnsafeQuery,
            StrictSchema = branch.StrictSchema,
            NoSchemaValidation = branch.NoSchemaValidation,
            AutoMigrate = branch.AutoMigrate,
            PreExec = branch.PreExec,
            PostExec = branch.PostExec,
            OnErrorExec = branch.OnErrorExec,
            FinallyExec = branch.FinallyExec,

            From = branch.From.FirstOrDefault(),
            Ref = branch.Ref.ToArray(),
            Arguments = branch.RawArgs,

            Transformers = new List<TransformerConfig>(),
            ProviderOptions = new Dictionary<string, Dictionary<string, object>>(StringComparer.OrdinalIgnoreCase)
        };
    }
}
