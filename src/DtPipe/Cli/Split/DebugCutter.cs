using System.Security.Cryptography;
using System.Text;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Jobs;

namespace DtPipe.Cli.Split;

/// <summary>Where on a branch a watcher sits: at the read itself, or at the end of the branch's last transformer.</summary>
public enum DebugAt { Read, End }

/// <summary>
/// A card whose output a debug run keeps, named by the branch of the composed job that runs it. <see cref="Card"/> is
/// the page's own name for the card: a branch is renamed when cards are joined or split, the card is not.
/// The cutter reads the branch and the place only.
/// </summary>
public sealed record DebugWatch(string Branch, DebugAt At, string? Card = null);

/// <summary>A level a debug run kept: the checkpoint that holds what the branch had delivered after <see cref="Level"/> transformers.</summary>
public sealed record DebugHeld(string Branch, int Level, string Key);

/// <summary>
/// What a debug run knows of a branch: its transformers, and whether it can be cut inside. A branch with a
/// stream processor, or that <see cref="JobCutter"/> refuses (a literal secret, a block no side owns), runs
/// whole: only its end can be watched.
/// </summary>
public sealed record DebugBranch(string Branch, int Transformers, bool Cuttable);

/// <summary>
/// A level a watcher can sit on (0 is the read, the last is the end of the branch) and <see cref="Digest"/>, the hash of the text that produces it:
/// the branches it reads as written, its reader side and its first <see cref="Level"/> transformers, and the
/// rows the run reads. It does not depend on which levels are watched, so adding a watcher elsewhere leaves
/// every other level valid.
/// </summary>
public sealed record DebugLevel(string Branch, int Level, string Digest);

/// <summary>
/// The levels of a job, and who reads whom. <see cref="Resolve"/> turns a watcher into the level it sits on, or
/// null when its branch runs whole.
/// </summary>
public sealed record DebugPlan(IReadOnlyList<DebugBranch> Branches, IReadOnlyList<DebugLevel> Levels, IReadOnlyDictionary<string, IReadOnlySet<string>> Readers)
{
    public int? Resolve(DebugWatch watch)
    {
        var branch = Branches.FirstOrDefault(b => b.Branch.Equals(watch.Branch, StringComparison.OrdinalIgnoreCase));
        if (branch is null) return null;
        if (watch.At == DebugAt.End) return branch.Transformers;
        return branch.Cuttable || branch.Transformers == 0 ? 0 : null;
    }

    /// <summary>
    /// The digest of what a card's rows depend on. A read of a branch that runs whole has no level of its own, so it
    /// takes the digest of the branch's end: it changes whenever the branch does.
    /// </summary>
    public string? DigestAt(string branch, DebugAt at)
    {
        var info = Branches.FirstOrDefault(b => b.Branch.Equals(branch, StringComparison.OrdinalIgnoreCase));
        if (info is null) return null;
        var level = at == DebugAt.Read && (info.Cuttable || info.Transformers == 0) ? 0 : info.Transformers;
        return Levels.FirstOrDefault(l => l.Branch.Equals(branch, StringComparison.OrdinalIgnoreCase) && l.Level == level)?.Digest;
    }

    /// <summary>Every branch that reads <paramref name="branch"/>, directly or through others.</summary>
    public IReadOnlySet<string> Downstream(string branch)
    {
        var found = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pending = new Stack<string>([branch]);
        while (pending.Count > 0)
            if (Readers.TryGetValue(pending.Pop(), out var readers))
                foreach (var reader in readers) if (found.Add(reader)) pending.Push(reader);
        return found;
    }
}

/// <summary>The piece of the cut job that holds <see cref="Level"/> of a branch: it produced the level, or read it back.</summary>
public sealed record DebugPiece(string Branch, int Level, string Alias);

/// <summary>
/// The job for a debug run, as YAML, and the pieces that hold the watched levels. <see cref="Dropped"/> are the
/// branches the run leaves out; <see cref="Unread"/> are the branches that no longer read their source, since a
/// checkpoint stands for it: a binding that names either has nothing to bind.
/// </summary>
public sealed record DebugCut(string Yaml, IReadOnlyList<DebugPiece> Pieces, IReadOnlySet<string> Dropped, IReadOnlySet<string> Unread);

/// <summary>
/// Cuts the branches of a job at the levels a person watches into mini-branches joined by <c>from:</c>, each
/// keeping a checkpoint in a session of its own, so that a debug run retains what every watched level
/// delivered and a later one can start from the last it kept. The cut is JobCutter's: its reader side and
/// writer side (<see cref="JobCutter.Cut"/>, with <see cref="BranchSides"/>), never a second rule. The job
/// the person saved is never touched: the result is a job for one sample.
///
/// <para>
/// With no watcher nothing is cut. The head of a cut branch keeps the reader, its options, <c>limit</c> and
/// <c>sampling-*</c>; the last piece keeps the branch's alias, its output and the writer's keys, so a
/// <c>from:</c> or a <c>ref</c> elsewhere in the job still names it. A watcher on a branch that runs whole
/// (a stream processor, a branch <see cref="JobCutter"/> refuses) takes a checkpoint at the end of the branch,
/// with no cut.
/// </para>
///
/// <para>
/// A run can stop at a level and can start from the levels it holds: only the branches the stop needs run,
/// each from the highest level held at or below the one it must reach.
/// </para>
/// </summary>
public sealed class DebugCutter(
    IEnumerable<IStreamReaderFactory> readers, IEnumerable<IDataWriterFactory> writers, IEnumerable<IStreamTransformerFactory> processors)
{
    /// <summary>What a mini-branch's alias carries after the branch's own: the studio reserves it.</summary>
    public const string Mark = "__level";

    /// <summary>The session the debug runs of a studio keep their checkpoints in.</summary>
    public const string SessionName = "studio-debug";

    private sealed record Info(JobDefinition Job, int Transformers, bool Processor, bool Splittable)
    {
        public bool Cuttable => Splittable && Transformers > 0;
        public bool HasLevel(int level) => level == Transformers || Cuttable && level == 0;
    }

    private Dictionary<string, JobDefinition> Parse(string yaml)
    {
        // ${{…}} stays as typed: the cut is read back by the same resolvers as the original.
        var jobs = JobFileParser.ParseContent(yaml, null, interpolate: false);
        if (jobs.Keys.FirstOrDefault(a => a.Contains(Mark, StringComparison.Ordinal)) is { } reserved)
            throw new InvalidOperationException($"'{reserved}' holds '{Mark}', which a debug run reserves for its levels: rename the branch.");
        return jobs;
    }

    private Dictionary<string, Info> Inspect(Dictionary<string, JobDefinition> jobs) => jobs.ToDictionary(
        kv => kv.Key,
        kv =>
        {
            var processor = BranchDefinition.FromJob(kv.Key, kv.Value, processors).ProcessorName is not null;
            return new Info(kv.Value, kv.Value.Transformers?.Count ?? 0, processor, !processor && JobCutter.Inspect(jobs, kv.Key, readers, writers).IsCuttable);
        },
        StringComparer.OrdinalIgnoreCase);

    /// <summary>The aliases a branch reads: its <c>from</c> (a comma-separated list) and its <c>ref</c>.</summary>
    private static IEnumerable<string> Reads(JobDefinition job) =>
        (job.From?.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries) ?? []).Concat(job.Ref);

    /// <summary>Every level a watcher can sit on, with the digest of what produces it. Pure: nothing runs.</summary>
    public DebugPlan Plan(string yaml, int rows, string? salt = null)
    {
        var jobs = Parse(yaml);
        var infos = Inspect(jobs);
        var branches = new List<DebugBranch>();
        var levels = new List<DebugLevel>();
        foreach (var (alias, info) in infos)
        {
            branches.Add(new DebugBranch(alias, info.Transformers, info.Cuttable));
            foreach (var level in info.Cuttable ? new[] { 0, info.Transformers } : [info.Transformers])
                levels.Add(new DebugLevel(alias, level, Digest(jobs, alias, info, level, rows, salt)));
        }
        var readersOf = new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (alias, job) in jobs)
            foreach (var read in Reads(job))
                readersOf[read] = new HashSet<string>(readersOf.GetValueOrDefault(read) ?? new HashSet<string>(), StringComparer.OrdinalIgnoreCase) { alias };
        return new DebugPlan(branches, levels, readersOf);
    }

    /// <summary>
    /// The job to run. <paramref name="upTo"/> stops the run at that level, and drops what it does not need;
    /// <paramref name="held"/> are the levels the run may read back instead of producing.
    /// </summary>
    /// <param name="write">The run is the job's own, not a sample: the writer is not neutralised by the engine, so a stop
    /// is a piece with no writer at all, and the cursor and its state file stay with the piece that writes.</param>
    /// <exception cref="InvalidOperationException">The stop names no branch, or one that runs whole at that level.</exception>
    public DebugCut Cut(string yaml, IEnumerable<DebugWatch> watch, DebugWatch? upTo = null, IReadOnlyList<DebugHeld>? held = null, bool write = false)
    {
        var jobs = Parse(yaml);
        var infos = Inspect(jobs);
        held ??= [];

        var watched = new HashSet<(string Branch, int Level)>();
        foreach (var w in watch)
            if (infos.TryGetValue(w.Branch, out var info) && Resolve(info, w.At) is { } level) watched.Add((Canonical(jobs, w.Branch), level));

        (string Branch, int Level)? target = null;
        if (upTo is not null)
        {
            var info = infos.GetValueOrDefault(upTo.Branch) ?? throw new InvalidOperationException($"The job has no branch '{upTo.Branch}'.");
            var level = Resolve(info, upTo.At) ?? throw new InvalidOperationException(
                $"'{upTo.Branch}' runs whole: it can be watched at its end only, not at its read.");
            target = (Canonical(jobs, upTo.Branch), level);
            watched.Add(target.Value);
        }

        // What must run: the target (or every branch), each from the highest level held at or below the one it must
        // reach; a branch read back needs nothing upstream of the level.
        var plan = new Dictionary<string, (int Stop, int? From, string? Key)>(StringComparer.OrdinalIgnoreCase);
        void Need(string alias, int stop)
        {
            if (!infos.TryGetValue(alias, out var info) || plan.ContainsKey(alias)) return;
            var back = held.Where(h => h.Branch.Equals(alias, StringComparison.OrdinalIgnoreCase) && h.Level <= stop && info.HasLevel(h.Level))
                .OrderByDescending(h => h.Level).FirstOrDefault();
            plan[alias] = (stop, back?.Level, back?.Key);
            if (back is null) foreach (var read in Reads(info.Job)) Need(read, infos.GetValueOrDefault(read)?.Transformers ?? 0);
        }
        if (target is { } t) Need(t.Branch, t.Level);
        else
        {
            // The end of the job: every branch that writes or that nobody reads. One read back from a checkpoint
            // leaves its upstream out, since nothing would read what that upstream delivers.
            var read = infos.Values.SelectMany(i => Reads(i.Job)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            foreach (var (alias, info) in infos)
                if (!read.Contains(alias) || !string.IsNullOrEmpty(info.Job.Output)) Need(alias, info.Transformers);
        }

        var result = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase);
        var pieces = new List<DebugPiece>();
        var dropped = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var unread = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (alias, job) in jobs)
        {
            if (!plan.TryGetValue(alias, out var run)) { dropped.Add(alias); continue; }
            if (run.From is not null) unread.Add(alias);
            Emit(jobs, alias, infos[alias], run.Stop, run.From, run.Key, target?.Branch.Equals(alias, StringComparison.OrdinalIgnoreCase) == true,
                write && target is not null, write, watched, result, pieces);
        }
        return new DebugCut(JobFileWriter.Serialize(result), pieces, dropped, unread);
    }

    private static string Canonical(Dictionary<string, JobDefinition> jobs, string alias) =>
        jobs.Keys.First(k => k.Equals(alias, StringComparison.OrdinalIgnoreCase));

    private static int? Resolve(Info info, DebugAt at) =>
        at == DebugAt.End ? info.Transformers : info.Cuttable || info.Transformers == 0 ? 0 : null;

    private void Emit(
        Dictionary<string, JobDefinition> jobs, string alias, Info info, int stop, int? from, string? key, bool isTarget, bool silent, bool write,
        HashSet<(string Branch, int Level)> watched, Dictionary<string, JobDefinition> result, List<DebugPiece> pieces)
    {
        EmitPieces(jobs, alias, info, stop, from, key, isTarget, silent, write, watched, result, pieces);
        if (!write) return;

        // The cursor tracks what the writer wrote, and its state file is read as the next run's start: it advances
        // with the piece that writes the branch's target, never with a piece that only keeps a checkpoint.
        var job = info.Job;
        var writer = !silent && result.ContainsKey(alias) ? alias : null;
        foreach (var name in result.Keys.Where(k => k.Equals(alias, StringComparison.OrdinalIgnoreCase) || k.StartsWith(alias + Mark, StringComparison.OrdinalIgnoreCase)).ToList())
            result[name] = result[name] with
            {
                Cursor = name.Equals(writer, StringComparison.OrdinalIgnoreCase) ? job.Cursor : null,
                State = name.Equals(writer, StringComparison.OrdinalIgnoreCase) ? job.State : null,
            };
    }

    private void EmitPieces(
        Dictionary<string, JobDefinition> jobs, string alias, Info info, int stop, int? from, string? key, bool isTarget, bool silent, bool write,
        HashSet<(string Branch, int Level)> watched, Dictionary<string, JobDefinition> result, List<DebugPiece> pieces)
    {
        var job = info.Job;
        var count = info.Transformers;
        bool Watches(int level) => watched.Contains((alias, level));
        string Checkpoint(int level) => $"level-{level}";
        string Name(int level) => level == count ? alias : $"{alias}{Mark}{level}";
        // A target nothing reads and that writes nowhere would be refused by the engine: its rows are the answer. In a
        // sample the engine neutralises the writer, so the last piece may keep its output; a real run has no such net:
        // while it stops, no branch of it has a writer, or the stop is refused.
        JobDefinition Terminal(JobDefinition piece, bool last, bool producerForm)
        {
            if (silent)
            {
                // A branch that only feeds another has no output to neutralise and gets none.
                var output = isTarget || !string.IsNullOrEmpty(job.Output) ? "null:" : null;
                return producerForm ? piece with { Output = output } : JobCutter.Without(piece, BranchSides.Write) with { Output = output, ProviderOptions = null };
            }
            if (!isTarget) return piece;
            return last && !string.IsNullOrEmpty(piece.Output) ? piece : piece with { Output = "null:" };
        }
        // A branch that runs whole cannot be stopped before its writer.
        var detach = silent && stop == count;
        if (detach && !info.Splittable && !string.IsNullOrEmpty(job.Output))
            throw new InvalidOperationException($"'{alias}' runs whole: it cannot be stopped before its writer.");

        if (!info.Cuttable && !(detach && info.Splittable && from is null))
        {
            if (from is { } && key is not null)
            {
                result[alias] = Terminal(ReadBack(jobs, alias, info, key, last: true), last: true, producerForm: false);
                pieces.Add(new DebugPiece(alias, count, alias));
                return;
            }
            var whole = Watches(count) ? job with { Checkpoint = Checkpoint(count), Session = SessionName } : job;
            result[alias] = Terminal(whole, last: true, producerForm: true);
            if (Watches(count)) pieces.Add(new DebugPiece(alias, count, alias));
            return;
        }

        var start = from ?? -1;
        var ends = new[] { 0 }.Where(l => l > start && l < stop && Watches(l)).Append(stop).Where(l => l > start).ToList();
        if (start < 0 && ends is [var only] && only == count && !(detach && info.Splittable))
        {
            // No watcher inside: the branch runs as the job states it, with a checkpoint at its end when watched.
            var whole = Watches(count) ? job with { Checkpoint = Checkpoint(count), Session = SessionName } : job;
            result[alias] = Terminal(whole, last: true, producerForm: true);
            if (Watches(count)) pieces.Add(new DebugPiece(alias, count, alias));
            return;
        }

        string? previous = null;
        var previousLevel = start;
        if (start >= 0)
        {
            var last = start == count;
            var back = ReadBack(jobs, alias, info, key!, last);
            previous = Name(start);
            result[previous] = start == stop ? Terminal(back, last, producerForm: false) : back;
            pieces.Add(new DebugPiece(alias, start, previous));
        }
        foreach (var end in ends)
        {
            var last = end == count;
            var name = Name(end);
            // A watcher sits at the read or at the end, so a branch has at most two pieces: the head, then the tail.
            var piece = previous is null
                ? JobCutter.Cut(jobs, alias, end, readers, writers).Producer[alias] with { Output = null }
                : JobCutter.Cut(jobs, alias, previousLevel, readers, writers).Consumer[alias] with { Input = null, Ref = [], From = previous };
            piece = piece with { Session = SessionName, Checkpoint = Watches(end) ? Checkpoint(end) : null };
            result[name] = end == stop ? Terminal(piece, last, producerForm: previous is null) : piece;
            if (Watches(end)) pieces.Add(new DebugPiece(alias, end, name));
            previous = name;
            previousLevel = end;
        }
    }

    /// <summary>
    /// The branch's level read from a checkpoint: no read, no transformer, no checkpoint of its own, and
    /// nothing that bounds or samples a reader. The last level keeps the writer.
    /// </summary>
    private JobDefinition ReadBack(Dictionary<string, JobDefinition> jobs, string alias, Info info, string key, bool last)
    {
        var job = info.Job;
        JobDefinition piece;
        if (!last)
            piece = JobCutter.Without(job, BranchSides.Read.Concat(BranchSides.Write).Concat(BranchSides.Diagnostics)) with { Output = null, ProviderOptions = null };
        else if (info.Splittable)
            piece = JobCutter.Cut(jobs, alias, info.Transformers, readers, writers).Consumer[alias];
        else
        {
            // Whole, with a stream processor: what it read is the checkpoint now, so its own block goes.
            var processor = BranchDefinition.FromJob(alias, job, processors).ProcessorName;
            var options = job.ProviderOptions?.Where(kv => !kv.Key.Equals(processor, StringComparison.OrdinalIgnoreCase))
                .ToDictionary(kv => kv.Key, kv => kv.Value);
            piece = JobCutter.Without(job, BranchSides.Read.Concat(BranchSides.Diagnostics)) with { ProviderOptions = options is { Count: > 0 } ? options : null };
        }
        return piece with { Input = null, From = null, Ref = [], Transformers = null, FromCheckpoint = key, Checkpoint = null, Session = SessionName };
    }

    /// <summary>
    /// The text of everything that produced <paramref name="level"/> of a branch: every branch it reads as the job
    /// states them, its reader side and its first transformers, and the rows the run reads. Hashed, so no secret
    /// is kept.
    /// </summary>
    private string Digest(Dictionary<string, JobDefinition> jobs, string alias, Info info, int level, int rows, string? salt)
    {
        var parts = new Dictionary<string, JobDefinition>(StringComparer.Ordinal);
        var pending = new Stack<string>(Reads(info.Job));
        while (pending.Count > 0)
        {
            var upstream = pending.Pop();
            if (parts.ContainsKey(upstream) || !jobs.TryGetValue(upstream, out var u)) continue;
            parts[upstream] = u;
            foreach (var read in Reads(u)) pending.Push(read);
        }
        parts[alias] = info.Cuttable
            ? JobCutter.Cut(jobs, alias, level, readers, writers).Producer[alias] with { Output = null }
            : JobCutter.Without(info.Job, BranchSides.Write.Concat(BranchSides.Diagnostics));
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes($"rows={rows};salt={salt};level={level}\n{JobFileWriter.Serialize(parts)}")));
    }
}
