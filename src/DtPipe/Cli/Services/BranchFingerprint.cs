using System.Collections;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Core.Security;

namespace DtPipe.Cli.Services;

/// <summary>
/// What defines the rows a branch produces, as one hash: its source, its query, every
/// transformer with its configuration in order, a stream processor's own options, its limits and
/// sampling, and the fingerprint of each branch it reads from. The checkpoint key hashes it, so a
/// prefix that changes anywhere in the DAG changes the key of everything downstream.
///
/// <para>
/// Computed from the job as written, before the run: branches run concurrently and an upstream
/// without <c>--checkpoint</c> never computes a key, so no runtime handoff could supply it.
/// </para>
///
/// <para>
/// <b>Left out on purpose</b>: the alias that labels a branch (<c>--alias</c>, <c>--from</c>,
/// <c>--ref</c>, the job's key), where the rows go (the output and the writer's arguments) and what
/// they are named in the store (<c>--checkpoint</c>, <c>--session</c>). A plain <c>from:</c> branch
/// contributes its upstream's fingerprint alone, so renaming an alias moves no key. A stream
/// processor names its inputs inside its query, so it contributes (alias, fingerprint) pairs.
/// </para>
///
/// <para>
/// <b>Left in, which can only cost a cache miss, never a collision</b>: the writer's
/// <c>provider-options</c> share their key with the reader's, so they enter too; two spellings of
/// one step (a flag and a YAML block) hash apart.
/// </para>
///
/// <para>
/// <b>Secrets.</b> The text is hashed, never stored. The source goes through <c>Redact</c> (the job
/// carries it, so its argument is dropped from the slices), every other free-text value through
/// <c>Sanitize</c> first. A job read
/// from YAML arrives already interpolated, so a <c>${{…}}</c> value is hashed resolved, masked only
/// where its key looks sensitive. A value starting with <c>@</c> that names a file contributes the
/// file's content: a script that changed is a transformer that changed.
/// </para>
/// </summary>
internal static class BranchFingerprint
{
    /// <summary>
    /// The fingerprint of every branch of a DAG, by alias. A branch whose upstream is absent from
    /// <paramref name="jobs"/> contributes the missing alias by name only.
    /// </summary>
    public static IReadOnlyDictionary<string, string> ForDag(
        JobDagDefinition dag,
        IReadOnlyDictionary<string, JobDefinition> jobs,
        IReadOnlyDictionary<string, CliJobContext> contexts)
    {
        var branches = dag.Branches.ToDictionary(b => b.Alias, StringComparer.OrdinalIgnoreCase);
        var done = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var visiting = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        string Resolve(string alias)
        {
            if (done.TryGetValue(alias, out var known)) return known;
            if (!branches.TryGetValue(alias, out var branch) || !jobs.TryGetValue(alias, out var job))
                return "unknown:" + alias;
            // A cycle is refused by validation before any run; a name stops the recursion here.
            if (!visiting.Add(alias)) return "cycle:" + alias;

            contexts.TryGetValue(alias, out var context);
            var upstream = branch.StreamingAliases.Concat(branch.RefAliases)
                .Select(a => (Alias: a, Fingerprint: Resolve(a)))
                .ToList();

            visiting.Remove(alias);
            return done[alias] = Of(job, context, upstream, namesInputs: branch.HasStreamTransformer);
        }

        foreach (var branch in dag.Branches) Resolve(branch.Alias);
        return done;
    }

    /// <summary>
    /// The fingerprint of one branch.
    /// </summary>
    /// <param name="namesInputs">True for a stream processor, whose query names its inputs: the
    /// aliases then enter, sorted. False for a plain <c>from:</c>, which only reads its upstream's
    /// rows and keeps no trace of what the upstream was called.</param>
    public static string Of(
        JobDefinition job,
        CliJobContext? context,
        IReadOnlyList<(string Alias, string Fingerprint)> upstream,
        bool namesInputs)
    {
        var sb = new StringBuilder();
        Part(sb, "job", Canonical(job));

        // The CLI slices carry what the job leaves to the factories (transformer flags, --sql).
        // Aliases and the writer's slice are not part of what produces the rows.
        Part(sb, "reader", Arguments(context?.ReaderArguments));
        Part(sb, "pipeline", Arguments(context?.PipelineArguments));

        var inputs = namesInputs
            ? upstream.OrderBy(u => u.Alias, StringComparer.OrdinalIgnoreCase).Select(u => $"{u.Alias}={u.Fingerprint}")
            : upstream.Select(u => u.Fingerprint);
        foreach (var (input, index) in inputs.Select((value, i) => (value, i)))
            Part(sb, $"in{index}", input);

        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString())));
    }

    private static string Canonical(JobDefinition job)
    {
        var sb = new StringBuilder();
        Part(sb, "input", ConnectionStringSanitizer.Redact(job.Input));
        Part(sb, "from-checkpoint", job.FromCheckpoint ?? "");
        Part(sb, "from-contract", job.FromContract ?? "");
        Part(sb, "batch", job.BatchSize.ToString(CultureInfo.InvariantCulture));
        Part(sb, "bytes", job.MaxBatchBytes.ToString(CultureInfo.InvariantCulture));
        Part(sb, "limit", job.Limit.ToString(CultureInfo.InvariantCulture));
        Part(sb, "dry-run", job.DryRunCount.ToString(CultureInfo.InvariantCulture));
        Part(sb, "rate", job.SamplingRate.ToString("R", CultureInfo.InvariantCulture));
        Part(sb, "seed", job.SamplingSeed?.ToString(CultureInfo.InvariantCulture) ?? "");

        foreach (var (t, index) in (job.Transformers ?? new List<TransformerConfig>()).Select((value, i) => (value, i)))
        {
            Part(sb, $"t{index}", t.Type);
            Part(sb, $"t{index}.mappings", Map(t.Mappings));
            Part(sb, $"t{index}.options", Map(t.Options));
        }

        if (job.ProviderOptions is { } providers)
            foreach (var provider in providers.OrderBy(p => p.Key, StringComparer.Ordinal))
                Part(sb, $"provider.{provider.Key}", Map(provider.Value));

        return sb.ToString();
    }

    private static string Map<TValue>(IReadOnlyDictionary<string, TValue>? map)
        => map is null
            ? ""
            : string.Join('\n', map.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => Entry(kv.Key, kv.Value)));

    private static string Entry(string key, object? value)
        => ConnectionStringSanitizer.Sanitize($"{key}={Value(value)}");

    private static string Value(object? value) => value switch
    {
        null => "",
        string s => WithFileContent(s),
        IDictionary dictionary => "{" + string.Join(',', dictionary.Keys.Cast<object>()
            .OrderBy(k => k.ToString(), StringComparer.Ordinal)
            .Select(k => Entry(k.ToString() ?? "", dictionary[k]))) + "}",
        IEnumerable list => "[" + string.Join(',', list.Cast<object?>().Select(Value)) + "]",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    /// <summary>
    /// The branch's own arguments with the flags that label or place it dropped, in order.
    /// </summary>
    private static string Arguments(string[]? args)
    {
        if (args is null || args.Length == 0) return "";

        var kept = new List<string>();
        for (var i = 0; i < args.Length; i++)
        {
            if (Dropped.Contains(args[i]))
            {
                i++; // the flag's value
                continue;
            }
            kept.Add(args[i]);
        }

        return string.Join('\n', kept.Select(a => ConnectionStringSanitizer.Sanitize(WithFileContent(a))));
    }

    private static readonly HashSet<string> Dropped = new(StringComparer.OrdinalIgnoreCase)
    {
        "--alias", "--from", "--ref", "--checkpoint", "--session",
        "-o", "--output", "--metrics-path", "--log",
        // The source is a connection string: the job carries it, and it goes through Redact there.
        "-i", "--input",
    };

    private static string WithFileContent(string value)
    {
        if (value.Length < 2 || value[0] != '@') return value;
        try
        {
            var path = value[1..];
            return File.Exists(path)
                ? value + "#" + Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(path)))
                : value;
        }
        catch (IOException) { return value; }
        catch (UnauthorizedAccessException) { return value; }
    }

    /// <summary>
    /// Length-prefixed, like <c>CheckpointKey</c>: two different parts never serialise alike.
    /// </summary>
    private static void Part(StringBuilder sb, string name, string value)
        => sb.Append(name).Append(':').Append(value.Length).Append(':').Append(value).Append('\n');
}
