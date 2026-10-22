using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.Logging;
using YamlDotNet.RepresentationModel;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// The bricks this node offers. It reads its own databases through the dtpipe binary, with the
/// environment its fragments inherit: a source's schema, a preview
/// and a sample come from a bounded read, asked for, a source's verdict on its pinned contract from that read and
/// <c>dtpipe contract diff</c>, a sink's verdict on a contract from <c>dtpipe contract check</c>.
/// The coordinator never opens a brick's database for this.
/// </summary>
internal sealed class BrickHost(string node, IReadOnlyList<BrickDeclaration> declared, DtPipeCommand dtpipe, ILogger logger)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    /// <summary>A brick the node offers: what the owner declared, text included, which never leaves the node, and what is announced of it.</summary>
    private sealed record Offered(BrickDeclaration Declaration, BrickInfo Announced);

    private IReadOnlyList<Offered> _offered = [];

    /// <summary>
    /// What the node announces: each brick as the coordinator needs it and nothing more (no text, no schema), with the
    /// version its text gives.
    /// </summary>
    public IReadOnlyList<BrickInfo> Bricks { get; private set; } = [];

    /// <summary>The branch of the brick <paramref name="id"/> as its owner declared it, or <c>null</c> when none of that kind is offered.</summary>
    public YamlMappingNode? DeclaredBranch(string id, BrickKind kind) =>
        _offered.FirstOrDefault(o => o.Announced.Id == id && o.Announced.Kind == kind) is { } offered ? BranchOf(offered.Declaration) : null;

    /// <summary>The version of the brick <paramref name="id"/> as the node declares it, the one it announces.</summary>
    public string? DeclaredVersion(string id, BrickKind kind) => FragmentJob.VersionsOver(Bricks)(id, kind);

    /// <summary>What every offered brick is, for <see cref="BrickRules"/>; set once with <see cref="Bricks"/>.</summary>
    public IReadOnlyList<BrickForm> Forms { get; private set; } = [];

    /// <summary>
    /// Reads the declarations. A source's schema is not read here: it needs the database, and the coordinator asks for it
    /// when a person looks at the brick (<see cref="PreviewAsync"/>), so a node that starts reads no data.
    /// </summary>
    public Task InspectAsync(CancellationToken ct)
    {
        var offered = new List<Offered>();
        foreach (var brick in declared)
        {
            // A brick this node would refuse to run must not be offered: the designer would place it here.
            if (BrickRules.DeclaredRefusal(brick.Id, brick.BranchJson) is { } refusal)
            {
                logger.LogWarning("Brick {Brick} is not offered: {Reason}", brick.Id, refusal);
                continue;
            }
            var version = BrickRules.VersionOf(brick.Kind, brick.BranchJson);
            offered.Add(new Offered(brick, new BrickInfo(brick.Id, brick.Kind, brick.Title, brick.Description ?? "", version)));
        }
        _offered = offered;
        Bricks = offered.Select(o => o.Announced).ToList();
        Forms = offered.Select(o => BrickRules.Declared(o.Declaration.Kind, o.Declaration.BranchJson)).ToList();
        return Task.CompletedTask;
    }

    /// <summary>A source's first rows, as its declared branch reads them, or what a sink's table holds now.</summary>
    public async Task<BrickPreview> PreviewAsync(string id, int rows)
    {
        var brick = declared.FirstOrDefault(b => b.Id == id);
        if (brick is null) return new BrickPreview(null, $"{node} offers no brick '{id}'.");
        if (brick.Kind == BrickKind.Source) return await PreviewSourceAsync(id, rows);
        try
        {
            var branch = Branch(brick);
            var input = Scalar(branch, "output") ?? throw new InvalidOperationException("The sink names no output.");
            var table = ProviderOption(branch, "-writer", "table") ?? throw new InvalidOperationException("The sink names no table.");
            var result = await dtpipe.RunAsync(
                ["--input", input, "--query", $"SELECT * FROM {table}", "--limit", rows.ToString(), "--output", "csv:-", "--no-stats"],
                CancellationToken.None);
            return result.Succeeded ? new BrickPreview(result.Stdout, null) : new BrickPreview(null, result.Diagnostic);
        }
        catch (Exception ex)
        {
            return new BrickPreview(null, ex.Message);
        }
    }

    /// <summary>
    /// A source brick's preview is its sample: the declared branch run to <c>arrow:-</c> with all its
    /// options, bounded like a sample. The types and the values are then read from those same bytes, the
    /// types by <c>dtpipe inspect</c> and the values as CSV, so a column's type is the Arrow's own.
    /// </summary>
    private async Task<BrickPreview> PreviewSourceAsync(string id, int rows)
    {
        var sample = await SampleAsync(id, rows);
        if (sample.Arrow is not { } arrow) return new BrickPreview(null, sample.Error);
        var dir = Path.Combine(dtpipe.WorkingDirectory, "bricks", Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            var file = Path.Combine(dir, "preview.arrows");
            await File.WriteAllBytesAsync(file, arrow);
            var types = await dtpipe.RunAsync(["inspect", "--input", "arrow:" + file, "--format", "json"], CancellationToken.None);
            if (!types.Succeeded) return new BrickPreview(null, types.Diagnostic);
            var values = await dtpipe.RunAsync(["--input", "arrow:" + file, "--output", "csv:-", "--no-stats"], CancellationToken.None);
            return values.Succeeded
                ? new BrickPreview(values.Stdout, null, JsonSerializer.Deserialize<List<BrickColumn>>(types.Stdout, Json))
                : new BrickPreview(null, values.Diagnostic);
        }
        catch (Exception ex)
        {
            return new BrickPreview(null, ex.Message);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }

    /// <summary>
    /// A source brick's first rows, read by the brick's own branch sent to <c>arrow:-</c>, exactly as
    /// its fragment sends the runner: the same reader and writer, so the coordinator checks a job
    /// against the schema the runner will receive. A real read bounded by <c>--limit</c>, like a
    /// preview, not a sample run: the source-side guarantees of <c>--dry-run</c> do not apply. The schema
    /// contract that read produced comes with it.
    /// </summary>
    public async Task<BrickSample> SampleAsync(string id, int rows)
    {
        var brick = _offered.FirstOrDefault(o => o.Announced.Id == id && o.Announced.Kind == BrickKind.Source)?.Declaration;
        if (brick is null) return new BrickSample(null, $"{node} offers no source brick '{id}'.");
        try
        {
            var (result, contract) = await RunBranchAsync(brick, "output", new Dictionary<string, string>(),
                job => ["--job", job, "--limit", rows.ToString(), "--no-stats"], saveContract: true);
            if (!result.Succeeded) return new BrickSample(null, result.Diagnostic);
            return result.Output.Length > MaxSampleBytes
                ? new BrickSample(null, $"{node}: the first {rows} row(s) of '{id}' take {result.Output.Length:N0} bytes, over the {MaxSampleBytes:N0} a sample may.")
                : new BrickSample(result.Output, null, contract);
        }
        catch (Exception ex)
        {
            return new BrickSample(null, ex.Message);
        }
    }

    /// <summary>
    /// Whether a source brick still produces what a binding was checked against: its sample is read as <see cref="SampleAsync"/>
    /// reads it, and the contract it produces is judged against <paramref name="pinnedContract"/>. <c>contract</c> accepts the
    /// same schema (the hash); <c>compatible</c> also a different one that still satisfies the pinned one's consumers
    /// (<c>dtpipe contract diff</c>). A sample or a comparison that fails is <see cref="SourceVerdicts.Unread"/>, never a verdict.
    /// </summary>
    public async Task<SourceCheck> CheckSourceAsync(string id, string pinnedContract, string mode)
    {
        if (mode is not (BindingModes.Contract or BindingModes.Compatible))
            return new SourceCheck(SourceVerdicts.Unread, $"A source is checked in mode '{BindingModes.Contract}' or '{BindingModes.Compatible}', not '{mode}'.");
        try
        {
            var sample = await SampleAsync(id, 1);
            if (sample.Arrow is null) return new SourceCheck(SourceVerdicts.Unread, sample.Error ?? $"{node} sent no sample of '{id}'.");
            if (sample.Contract is null) return new SourceCheck(SourceVerdicts.Unread, $"{node} captured no contract of '{id}'.");
            var (pinned, now) = (HashOf(pinnedContract), HashOf(sample.Contract));
            if (pinned is null || now is null) return new SourceCheck(SourceVerdicts.Unread, "A contract has no hash.");
            if (pinned == now) return new SourceCheck(SourceVerdicts.Same, "");

            var (compatible, report) = await DiffAsync(pinnedContract, sample.Contract);
            if (mode == BindingModes.Contract)
                return new SourceCheck(SourceVerdicts.Breaking, $"Its contract is no longer the pinned one ({pinned[..12]} → {now[..12]}).{(report.Length > 0 ? "\n" + report : "")}");
            return compatible switch
            {
                true => new SourceCheck(SourceVerdicts.Compatible, report),
                false => new SourceCheck(SourceVerdicts.Breaking, report),
                null => new SourceCheck(SourceVerdicts.Unread, report),
            };
        }
        catch (Exception ex)
        {
            return new SourceCheck(SourceVerdicts.Unread, ex.Message);
        }
    }

    private static string? HashOf(string contract)
    {
        try
        {
            using var document = JsonDocument.Parse(contract);
            var hash = document.RootElement.TryGetProperty("hash", out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;
            return hash is { Length: >= 12 } ? hash : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// <c>dtpipe contract diff --json</c>: whether <paramref name="now"/> still satisfies the consumers of <paramref name="pinned"/>
    /// and the changes that say why, or <c>null</c> when dtpipe answered nothing readable. Its exit code is not read: a command that
    /// fails exits as one that finds a break.
    /// </summary>
    private async Task<(bool? Compatible, string Report)> DiffAsync(string pinned, string now)
    {
        var dir = Path.Combine(dtpipe.WorkingDirectory, "bricks", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var (old, current) = (Path.Combine(dir, "pinned.contract.json"), Path.Combine(dir, "now.contract.json"));
            await File.WriteAllTextAsync(old, pinned);
            await File.WriteAllTextAsync(current, now);
            var result = await dtpipe.RunAsync(["contract", "diff", "--json", old, current], CancellationToken.None);
            try
            {
                using var document = JsonDocument.Parse(result.Stdout);
                var root = document.RootElement;
                if (!root.TryGetProperty("compatible", out var compatible) || compatible.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                    return (null, result.Diagnostic);
                var changes = root.TryGetProperty("changes", out var list) && list.ValueKind == JsonValueKind.Array ? list.EnumerateArray().ToList() : [];
                var breaking = changes.Where(c => c.TryGetProperty("breaking", out var b) && b.ValueKind == JsonValueKind.True).ToList();
                var shown = breaking.Count > 0 ? breaking : changes;
                var report = string.Join('\n', shown.Select(c =>
                    $"{(c.TryGetProperty("column", out var column) ? column.GetString() : "?")}: {(c.TryGetProperty("detail", out var detail) ? detail.GetString() : "changed")}"));
                return (compatible.GetBoolean(), report);
            }
            catch (JsonException)
            {
                return (null, result.Diagnostic);
            }
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }

    /// <summary>
    /// Whether a sink brick's target accepts a contract: <c>dtpipe contract check</c> on the brick fed
    /// from <c>arrow:-</c>, which inspects the target and writes nothing.
    /// </summary>
    public async Task<SinkCheck> CheckSinkAsync(string id, string contractJson)
    {
        var brick = _offered.FirstOrDefault(o => o.Announced.Id == id && o.Announced.Kind == BrickKind.Sink)?.Declaration;
        if (brick is null) return new SinkCheck(false, $"{node} offers no sink brick '{id}'.");
        try
        {
            var (result, _) = await RunBranchAsync(brick, "input", new Dictionary<string, string> { ["contract.json"] = contractJson },
                job => ["contract", "check", "--job", job, "--contract", Path.Combine(Path.GetDirectoryName(job)!, "contract.json")]);
            return new SinkCheck(result.Succeeded, Findings(result.Stderr) ?? result.Tail(12));
        }
        catch (Exception ex)
        {
            return new SinkCheck(false, ex.Message);
        }
    }

    /// <summary>dtpipe's findings, from its first <c>error:</c> or <c>warning:</c> line on, without the table above them.</summary>
    private static string? Findings(string stderr)
    {
        var lines = stderr.Split('\n').Select(l => l.TrimEnd()).Where(l => l.Length > 0).ToList();
        var first = lines.FindIndex(l => l.StartsWith("error:", StringComparison.Ordinal) || l.StartsWith("warning:", StringComparison.Ordinal));
        return first < 0 ? null : string.Join('\n', lines.Skip(first));
    }

    // A sample crosses the node channel as one message.
    private const int MaxSampleBytes = 256 * 1024;

    /// <summary>
    /// Runs dtpipe on a job holding the brick's branch alone, its <paramref name="endpoint"/> set to
    /// <c>arrow:-</c>, in a directory of its own with <paramref name="files"/> beside the job. With <paramref name="saveContract"/>
    /// the branch saves the contract it produces there, by an absolute path (a job's relative path is read against the process's
    /// directory, which a concurrent run shares), and it is read back before the directory goes.
    /// </summary>
    private async Task<(DtPipeCommandResult Result, string? Contract)> RunBranchAsync(
        BrickDeclaration brick, string endpoint, IReadOnlyDictionary<string, string> files, Func<string, string[]> args, bool saveContract = false)
    {
        var dir = Path.GetFullPath(Path.Combine(dtpipe.WorkingDirectory, "bricks", Guid.NewGuid().ToString("N")));
        Directory.CreateDirectory(dir);
        try
        {
            var branch = BranchOf(brick) ?? throw new InvalidOperationException($"The brick '{brick.Id}' declares no branch.");
            JobYaml.Set(branch, endpoint, BrickRules.ArrowEndpoint);
            var saved = Path.Combine(dir, "saved.contract.json");
            if (saveContract) JobYaml.Set(branch, "contract-save", saved);
            var job = Path.Combine(dir, "job.yaml");
            await File.WriteAllTextAsync(job, JobYaml.Serialize(new YamlMappingNode { { "brick", branch } }));
            foreach (var (name, content) in files) await File.WriteAllTextAsync(Path.Combine(dir, name), content);
            var result = await dtpipe.RunAsync(args(job), CancellationToken.None);
            return (result, saveContract && File.Exists(saved) ? await File.ReadAllTextAsync(saved) : null);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }

    /// <summary>The branch a declaration holds, or <c>null</c> when its text is no mapping.</summary>
    private static YamlMappingNode? BranchOf(BrickDeclaration brick)
    {
        try { return JobYaml.FromJson(JsonNode.Parse(brick.BranchJson)) as YamlMappingNode; }
        catch (Exception) { return null; }
    }

    private static JsonElement Branch(BrickDeclaration brick) => JsonDocument.Parse(brick.BranchJson).RootElement.Clone();

    private static string? Scalar(JsonElement branch, string key) =>
        branch.TryGetProperty(key, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    /// <summary>A setting of the first provider-options component whose name ends with <paramref name="suffix"/>.</summary>
    private static string? ProviderOption(JsonElement branch, string suffix, string key)
    {
        if (!branch.TryGetProperty("provider-options", out var components) || components.ValueKind != JsonValueKind.Object) return null;
        foreach (var component in components.EnumerateObject())
        {
            if (component.Name.EndsWith(suffix, StringComparison.Ordinal) && component.Value.ValueKind == JsonValueKind.Object
                && component.Value.TryGetProperty(key, out var value))
                return value.ToString();
        }
        return null;
    }
}
