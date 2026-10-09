using System.Security.Cryptography;
using System.Text;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Jobs;

namespace DtPipe.Cli.Split;

/// <summary>
/// One level of a debug run: what a branch had delivered after its first <see cref="Level"/> transformers
/// (0 is the read itself). <see cref="Alias"/> is the mini-branch that produced it, and <see cref="Digest"/>
/// stands for the text of everything that produced it, so a later change to any of it is told from a hash.
/// </summary>
public sealed record DebugLevel(string Branch, int Level, string Alias, string Digest);

/// <summary>
/// The job for a debug run, as YAML, and the levels it holds. <see cref="Dropped"/> are the branches a resume left
/// out because nothing reads them any more: a binding that names one has nothing to bind.
/// </summary>
public sealed record DebugCut(string Yaml, IReadOnlyList<DebugLevel> Levels, IReadOnlySet<string> Dropped);

/// <summary>The level to start from, and the checkpoint its earlier run kept.</summary>
public sealed record DebugResume(string Branch, int Level, string Key);

/// <summary>
/// Cuts every branch of a job after each of its transformers into mini-branches joined by <c>from:</c>,
/// each keeping a checkpoint in a session of its own, so that a debug run retains what every level
/// delivered and a later one can start from any of them. The cut is JobCutter's: its reader side and
/// writer side (<see cref="JobCutter.Cut"/>, with <see cref="BranchSides"/>), never a second rule. The job
/// the person saved is never touched: the result is a job for one sample.
///
/// <para>
/// The head keeps the reader, its options, <c>limit</c> and <c>sampling-*</c>; the last mini-branch keeps the
/// branch's alias, its output and the writer's keys, so a <c>from:</c> or a <c>ref</c> elsewhere in the job
/// still names it. A branch with a stream processor, with no transformer, or that <see cref="JobCutter"/>
/// refuses (a literal secret, a block no side owns) stays whole.
/// </para>
/// </summary>
public sealed class DebugCutter(
    IEnumerable<IStreamReaderFactory> readers, IEnumerable<IDataWriterFactory> writers, IEnumerable<IStreamTransformerFactory> processors)
{
    /// <summary>What a mini-branch's alias carries after the branch's own: the studio reserves it.</summary>
    public const string Mark = "__level";

    private const string ResumeInput = "generate:1";

    /// <summary>The session the debug runs of a studio keep their checkpoints in.</summary>
    public const string SessionName = "studio-debug";

    public DebugCut Cut(string yaml, DebugResume? resume = null)
    {
        // ${{…}} stays as typed: the cut is read back by the same resolvers as the original.
        var jobs = JobFileParser.ParseContent(yaml, null, interpolate: false);
        if (jobs.Keys.FirstOrDefault(a => a.Contains(Mark, StringComparison.Ordinal)) is { } reserved)
            throw new InvalidOperationException($"'{reserved}' holds '{Mark}', which a debug run reserves for its levels: rename the branch.");
        var processorBranches = jobs
            .Where(kv => BranchDefinition.FromJob(kv.Key, kv.Value, processors).ProcessorName is not null)
            .Select(kv => kv.Key).ToHashSet(StringComparer.OrdinalIgnoreCase);

        var result = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase);
        var levels = new List<DebugLevel>();
        foreach (var (alias, job) in jobs)
        {
            if (processorBranches.Contains(alias) || job.Transformers is not { Count: > 0 } || !JobCutter.Inspect(jobs, alias, readers, writers).IsCuttable)
            {
                result[alias] = job;
                continue;
            }
            var chain = Chain(jobs, alias, job);
            for (var k = 0; k < chain.Count; k++)
            {
                var digest = Digest(jobs, job, chain, k);
                levels.Add(new DebugLevel(alias, k, chain[k].Alias, digest));
            }
            var from = resume is not null && resume.Branch.Equals(alias, StringComparison.OrdinalIgnoreCase) ? resume.Level : -1;
            for (var k = Math.Max(from, 0); k < chain.Count; k++)
            {
                var mini = chain[k].Job;
                if (k == from)
                {
                    // The level read back: no read, no transformer, no new checkpoint of its own, and nothing that
                    // bounds or samples a reader. The last level keeps its writer.
                    var last = k == chain.Count - 1;
                    // The job validator asks every branch for an input, a 'from' or a processor and does not count
                    // 'from-checkpoint'; the engine reads the checkpoint and ignores the input, so a stand-in does.
                    mini = JobCutter.Without(mini, BranchSides.Read) with
                    {
                        Input = ResumeInput, Transformers = null, Checkpoint = null, FromCheckpoint = resume!.Key, From = null, Ref = [],
                        ProviderOptions = last ? mini.ProviderOptions : null, Session = SessionName,
                    };
                }
                result[chain[k].Alias] = mini;
            }
        }
        var dropped = resume is not null ? Prune(result) : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        return new DebugCut(JobFileWriter.Serialize(result), levels, dropped);
    }

    /// <summary>The mini-branches of one branch, level 0 first; the last carries the branch's alias.</summary>
    private List<(string Alias, JobDefinition Job)> Chain(Dictionary<string, JobDefinition> jobs, string alias, JobDefinition job)
    {
        var count = job.Transformers!.Count;
        var chain = new List<(string, JobDefinition)>();
        var head = JobCutter.Cut(jobs, alias, 0, readers, writers).Producer[alias];
        chain.Add((count == 0 ? alias : $"{alias}{Mark}0", head with { Output = null, Session = SessionName, Checkpoint = "level-0" }));
        for (var k = 1; k < count; k++)
        {
            var middle = JobCutter.Without(job, BranchSides.Read.Concat(BranchSides.Write).Concat(BranchSides.Diagnostics)) with
            {
                Input = null, Output = null, ProviderOptions = null, Ref = [],
                Transformers = [job.Transformers[k - 1]],
                From = chain[^1].Item1, Session = SessionName, Checkpoint = $"level-{k}",
            };
            chain.Add(($"{alias}{Mark}{k}", middle));
        }
        var tail = JobCutter.Cut(jobs, alias, count - 1, readers, writers).Consumer[alias];
        chain.Add((alias, tail with { Input = null, Ref = [], From = chain[^1].Item1, Session = SessionName, Checkpoint = $"level-{count}" }));
        return chain;
    }

    /// <summary>
    /// The text of everything that produced level <paramref name="level"/> of the branch: the mini-branches up to
    /// it, and every branch the first reads, as the job states them. Hashed, so no secret is kept.
    /// </summary>
    private static string Digest(Dictionary<string, JobDefinition> jobs, JobDefinition original, List<(string Alias, JobDefinition Job)> chain, int level)
    {
        var parts = new Dictionary<string, JobDefinition>(StringComparer.Ordinal);
        var pending = new Stack<string>(chain[0].Job.From is { } f ? [f, .. chain[0].Job.Ref] : chain[0].Job.Ref);
        while (pending.Count > 0)
        {
            var upstream = pending.Pop();
            if (parts.ContainsKey(upstream) || !jobs.TryGetValue(upstream, out var u)) continue;
            parts[upstream] = u;
            if (u.From is { } more) pending.Push(more);
            foreach (var r in u.Ref) pending.Push(r);
        }
        for (var k = 0; k <= level; k++)
            parts[chain[k].Alias] = chain[k].Job with { Checkpoint = null };
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(JobFileWriter.Serialize(parts))));
    }

    /// <summary>
    /// A resumed job no longer reads what its first levels read: a branch that then has no output and that
    /// nothing reads is dropped, since the engine refuses a branch whose rows would be discarded.
    /// </summary>
    private static HashSet<string> Prune(Dictionary<string, JobDefinition> jobs)
    {
        var dropped = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (true)
        {
            var read = jobs.Values.SelectMany(j => j.From is null ? j.Ref : [j.From, .. j.Ref]).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var useless = jobs.Where(kv => string.IsNullOrEmpty(kv.Value.Output) && !read.Contains(kv.Key)).Select(kv => kv.Key).ToList();
            if (useless.Count == 0) return dropped;
            foreach (var alias in useless) { jobs.Remove(alias); dropped.Add(alias); }
        }
    }
}
