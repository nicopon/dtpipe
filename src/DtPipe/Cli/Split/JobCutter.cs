using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Security;

namespace DtPipe.Cli.Split;

/// <summary>Which half a value of the job ended up on.</summary>
public enum CutSide
{
    Producer,
    Consumer,

    /// <summary>Carried by neither half, and listed for it rather than disappearing.</summary>
    Dropped,
}

/// <summary>
/// One value the cut placed, as the acknowledgement prints it. <c>Display</c> is already safe to
/// show: a connection string is redacted where the placement is built, not where it is rendered.
/// </summary>
public sealed record CutPlacement(CutSide Side, string Key, string Display);

/// <summary>The two halves, what the cut placed on each, and what it refuses to place.</summary>
public sealed record CutResult(
    Dictionary<string, JobDefinition> Producer,
    Dictionary<string, JobDefinition> Consumer,
    IReadOnlyList<CutPlacement> Placements,
    IReadOnlyList<string> Literals,
    IReadOnlyList<string> UnplaceableOptionBlocks);

/// <summary>
/// The two grounds on which a job is refused, both of which are properties of the job as written
/// rather than of where it is cut — so they are decidable before it runs, and are.
/// </summary>
public sealed record CutInspection(
    IReadOnlyList<string> Literals,
    IReadOnlyList<string> UnplaceableOptionBlocks)
{
    public bool IsCuttable => Literals.Count == 0 && UnplaceableOptionBlocks.Count == 0;
}

/// <summary>
/// Splits one branch of a job in two at a stage boundary, joining the halves with an Arrow link.
///
/// <para>
/// <b>The producer is the cut branch alone; the consumer is the whole job with that branch's source
/// replaced by the link.</b> A single-branch job yields the obvious pair, and a DAG keeps its other
/// branches on the consumer side instead of being refused.
/// </para>
///
/// <para>
/// Every field crosses unchanged unless the cut determines its side: what governs reading goes to
/// the producer, what governs writing to the consumer. The two diagnostic paths go to neither —
/// one path cannot describe two runs — and the placement list says so rather than letting them
/// vanish.
/// </para>
///
/// <para>
/// A provider-options block is placed by which side's component owns it, resolved through
/// <see cref="ComponentSelector"/> like every other routing site. A block neither component owns is
/// not guessed at: a <c>--duck-init</c> mounting a third database may serve either half, and the
/// cut stops naming the block. Guessing is how a credential reaches the wrong team's repository,
/// which is the whole reason this gate exists.
/// </para>
/// </summary>
public static class JobCutter
{
    /// <summary>
    /// What the halves meet on. Arrow, and not a user choice: a text link loses the types — a
    /// <c>DECIMAL(18,4)</c> arrives as a <c>Double</c> through JSONL, and <c>NULL</c> cannot be told
    /// from an empty string through CSV — and with them the one schema both halves agree on, which
    /// is what a contract is derived from. Being no one's option is also what lets it be replaced
    /// by a hub without any command line changing.
    /// </summary>
    public const string Link = "arrow:-";

    /// <summary>
    /// Asks whether the job can be cut at all, without saying where. Neither ground depends on the
    /// cut index, so neither needs the run that establishes it.
    /// </summary>
    public static CutInspection Inspect(
        Dictionary<string, JobDefinition> jobs,
        string alias,
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataWriterFactory> writerFactories)
        => new(FindLiterals(jobs),
               SplitProviderOptions(jobs[alias], readerFactories, writerFactories).Unplaceable);

    public static CutResult Cut(
        Dictionary<string, JobDefinition> jobs,
        string alias,
        int at,
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataWriterFactory> writerFactories)
    {
        var job = jobs[alias];
        var transformers = job.Transformers ?? new List<TransformerConfig>();
        var upstream = transformers.Take(at).ToList();
        var downstream = transformers.Skip(at).ToList();

        var options = SplitProviderOptions(job, readerFactories, writerFactories);

        var producerJob = job with
        {
            Output = Link,
            Transformers = upstream.Count > 0 ? upstream : null,
            ProviderOptions = options.Reader,
            // Write side, and the producer has no target of its own.
            Prefix = null,
            Checkpoint = null,
            ContractSave = null,
            MetricsPath = null,
            LogPath = null,
        };

        var consumerJob = job with
        {
            Input = Link,
            Transformers = downstream.Count > 0 ? downstream : null,
            ProviderOptions = options.Writer,
            // Read side: the consumer reads a finished stream, so nothing here still has a source
            // to bound, resume from or sample.
            Cursor = null,
            State = null,
            Limit = 0,
            SamplingRate = 1.0,
            SamplingSeed = null,
            FromCheckpoint = null,
            FromContract = null,
            MetricsPath = null,
            LogPath = null,
        };

        var consumer = new Dictionary<string, JobDefinition>(jobs, StringComparer.OrdinalIgnoreCase)
        {
            [alias] = consumerJob,
        };
        var producer = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            [alias] = producerJob,
        };

        return new CutResult(
            producer,
            consumer,
            Describe(job, producerJob, consumerJob, upstream, downstream),
            FindLiterals(jobs),
            options.Unplaceable);
    }

    /// <summary>
    /// What the cut placed, in the order a reader checks it: the producer's side, the consumer's,
    /// then what it carried to neither.
    /// </summary>
    private static List<CutPlacement> Describe(
        JobDefinition job,
        JobDefinition producer,
        JobDefinition consumer,
        List<TransformerConfig> upstream,
        List<TransformerConfig> downstream)
    {
        var rows = new List<CutPlacement>();

        void Add(CutSide side, string key, string? value)
        {
            if (!string.IsNullOrEmpty(value)) rows.Add(new CutPlacement(side, key, value));
        }

        Add(CutSide.Producer, "input", ConnectionStringSanitizer.Redact(producer.Input));
        Add(CutSide.Producer, "transformers", Names(upstream));
        Add(CutSide.Producer, "cursor", producer.Cursor);
        Add(CutSide.Producer, "state", producer.State);
        Add(CutSide.Producer, "from-checkpoint", producer.FromCheckpoint);
        Add(CutSide.Producer, "from-contract", producer.FromContract);
        foreach (var block in Blocks(producer.ProviderOptions))
            rows.Add(new CutPlacement(CutSide.Producer, "provider-options", block));

        Add(CutSide.Consumer, "output", ConnectionStringSanitizer.Redact(consumer.Output));
        Add(CutSide.Consumer, "transformers", Names(downstream));
        Add(CutSide.Consumer, "prefix", consumer.Prefix);
        Add(CutSide.Consumer, "checkpoint", consumer.Checkpoint);
        Add(CutSide.Consumer, "contract-save", consumer.ContractSave);
        foreach (var block in Blocks(consumer.ProviderOptions))
            rows.Add(new CutPlacement(CutSide.Consumer, "provider-options", block));

        // A cut makes two runs out of one, and a single path cannot hold both their reports: the
        // second process would overwrite the first. Naming them here is the difference between a
        // decision and a loss.
        Add(CutSide.Dropped, "metrics-path", job.MetricsPath);
        Add(CutSide.Dropped, "log-path", job.LogPath);

        return rows;
    }

    private static string Names(List<TransformerConfig> configs)
        => configs.Count == 0 ? string.Empty : string.Join(", ", configs.Select(c => c.Type));

    private static IEnumerable<string> Blocks(Dictionary<string, Dictionary<string, object?>>? options)
        => options is null
            ? Enumerable.Empty<string>()
            : options.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                     .Select(kv => $"{kv.Key}: {string.Join(", ", kv.Value.Keys.OrderBy(k => k, StringComparer.Ordinal))}");

    /// <summary>
    /// Every credential the job spells out, across <b>all</b> its branches: the cut copies the
    /// branches it did not touch into the consumer file verbatim, so one of theirs lands in a
    /// repository just as surely as the cut branch's own.
    /// </summary>
    private static List<string> FindLiterals(Dictionary<string, JobDefinition> jobs)
    {
        var found = new List<string>();

        foreach (var (alias, job) in jobs.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            if (ConnectionStringSanitizer.CarriesLiteralSecret(job.Input)) found.Add($"{alias}.input");
            if (ConnectionStringSanitizer.CarriesLiteralSecret(job.Output)) found.Add($"{alias}.output");

            if (job.ProviderOptions is null) continue;
            foreach (var (block, entries) in job.ProviderOptions.OrderBy(kv => kv.Key, StringComparer.Ordinal))
                foreach (var (key, value) in entries.OrderBy(kv => kv.Key, StringComparer.Ordinal))
                    if (ConnectionStringSanitizer.CarriesLiteralSecret(value as string))
                        found.Add($"{alias}.provider-options.{block}.{key}");
        }

        return found;
    }

    private static (Dictionary<string, Dictionary<string, object?>>? Reader,
                    Dictionary<string, Dictionary<string, object?>>? Writer,
                    List<string> Unplaceable)
        SplitProviderOptions(
            JobDefinition job,
            IEnumerable<IStreamReaderFactory> readerFactories,
            IEnumerable<IDataWriterFactory> writerFactories)
    {
        var unplaceable = new List<string>();
        if (job.ProviderOptions is not { Count: > 0 }) return (null, null, unplaceable);

        var reader = PipelineToJobConverter.ResolveFactory(job.Input, readerFactories);
        var writer = PipelineToJobConverter.ResolveFactory(job.Output, writerFactories);

        var readerKeys = KeysOwnedBy(reader?.ComponentName, "-reader");
        var writerKeys = KeysOwnedBy(writer?.ComponentName, "-writer");

        // The plain key belongs to whichever side's component answers to it — unless both do, which
        // is why the exporter suffixes both entries when a csv: reader feeds a csv: writer. A plain
        // key that survives that case is owned by neither side in a way anyone can read off it.
        if (reader is not null && writer is not null
            && reader.ComponentName.Equals(writer.ComponentName, StringComparison.OrdinalIgnoreCase))
        {
            readerKeys.Remove(reader.ComponentName);
            writerKeys.Remove(writer.ComponentName);
        }

        var readerBlocks = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase);
        var writerBlocks = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase);

        foreach (var (key, entries) in job.ProviderOptions)
        {
            if (readerKeys.Contains(key)) readerBlocks[key] = entries;
            else if (writerKeys.Contains(key)) writerBlocks[key] = entries;
            else unplaceable.Add(key);
        }

        return (readerBlocks.Count > 0 ? readerBlocks : null,
                writerBlocks.Count > 0 ? writerBlocks : null,
                unplaceable);
    }

    private static HashSet<string> KeysOwnedBy(string? component, string suffix)
        => string.IsNullOrEmpty(component)
            ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            : new HashSet<string>(new[] { component, component + suffix }, StringComparer.OrdinalIgnoreCase);
}
