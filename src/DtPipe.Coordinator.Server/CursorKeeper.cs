using System.Text.Json;
using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>A brick a kept cursor was built on: its role (<c>sink</c> or <c>source</c>), its key and the version it had.</summary>
public sealed record CursorBasis(string Role, string Brick, string Version);

/// <summary>
/// A cursor as the coordinator keeps it, for the sink branch <see cref="Alias"/> of <see cref="Pipeline"/>. <see cref="Basis"/> is
/// the sink brick and the source brick upstream of it when the value was kept; null for a value a person set before any run, which the
/// first run adopts.
/// </summary>
public sealed record CursorRecord(
    string Pipeline, string Alias, string Column, string Type, string Value, IReadOnlyList<CursorBasis>? Basis, string? RunId,
    DateTimeOffset UpdatedAt, string? SetBy);

/// <summary>What became of one cursor a run reported: kept as <see cref="Value"/>, or not, and why.</summary>
public sealed record CursorOutcome(string Alias, bool Kept, string? Value, string? Reason);

/// <summary>
/// The cursors of the library's pipelines, one per branch that writes: the highest value its sink tracked in the last run that
/// succeeded as a whole. The coordinator resolves it when a run starts (the value goes to the fragment holding the source the sink is
/// fed from) and keeps the new one only at the verdict of the run, so a failed, cancelled or interrupted run advances nothing. A cursor
/// is tied to the versions of its sink brick (which holds the column) and its source brick: when either has changed, the run is
/// refused by name, and a person resets the cursor or sets a value. Memory is the truth, read once at start; the store is written first.
/// </summary>
public sealed class CursorKeeper
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly ICursorStore _store;
    private readonly BrickCatalog _bricks;
    private readonly ILogger _logger;
    private readonly object _lock = new();
    private readonly Dictionary<(string Pipeline, string Alias), CursorRecord> _cursors = [];

    public CursorKeeper(ICursorStore store, BrickCatalog bricks, ILogger<CursorKeeper> logger)
    {
        (_store, _bricks, _logger) = (store, bricks, logger);
        foreach (var stored in store.Load())
        {
            try
            {
                if (JsonSerializer.Deserialize<CursorRecord>(stored.Document, Json) is { } record
                    && record.Pipeline == stored.Pipeline && record.Alias == stored.Alias)
                    _cursors[(stored.Pipeline, stored.Alias)] = record;
                else
                    logger.LogWarning("Cursor {Pipeline}/{Alias} skipped: its document names another cursor", stored.Pipeline, stored.Alias);
            }
            catch (JsonException ex)
            {
                logger.LogWarning("Cursor {Pipeline}/{Alias} skipped: {Error}", stored.Pipeline, stored.Alias, ex.Message);
            }
        }
    }

    /// <summary>Whether the pipeline has a kept cursor: a launch while one of its runs is in flight is then refused, not queued.</summary>
    public bool Has(string pipeline)
    {
        lock (_lock) return _cursors.Keys.Any(k => k.Pipeline == pipeline);
    }

    /// <summary>The pipeline's cursors, by alias.</summary>
    public IReadOnlyList<CursorRecord> Of(string pipeline)
    {
        lock (_lock) return _cursors.Where(c => c.Key.Pipeline == pipeline).Select(c => c.Value).OrderBy(c => c.Alias, StringComparer.Ordinal).ToList();
    }

    /// <summary>
    /// What each fragment of <paramref name="plan"/> starts from: the kept value of every cursor of the pipeline, to the fragment that
    /// holds its source, keyed by fragment name. Throws <see cref="ConflictException"/> naming the cursor when it cannot be served: a
    /// brick changed since it was kept, its sink or its source is no longer what it was, or two cursors read from one fragment.
    /// </summary>
    public IReadOnlyDictionary<string, CursorLaunch> Launches(string pipeline, DistributionPlan plan)
    {
        var launches = new Dictionary<string, CursorLaunch>(StringComparer.Ordinal);
        var owners = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var record in Of(pipeline))
        {
            var shape = Shape.Of(plan, record.Alias) ?? throw new ConflictException(
                $"The cursor of '{record.Alias}' was kept for a branch that writes, and the plan of '{pipeline}' has none of that name: reset the cursor.");
            if (shape.Refusal is { } unserved) throw new ConflictException($"The cursor of '{record.Alias}' cannot be served: {unserved}");
            if (record.Basis is { } basis && Changed(record, basis, BasisOf(shape)) is { } changed) throw new ConflictException(changed);
            if (CursorValues.Refusal(record.Type, record.Value) is { } bad)
                throw new ConflictException($"The kept value of the cursor of '{record.Alias}' is refused: {bad} Reset the cursor or set a value.");
            if (owners.TryGetValue(shape.SourceFragment!, out var other))
                throw new ConflictException($"The cursors of '{other}' and '{record.Alias}' both start from {shape.SourceFragment}, which takes one: reset one of them.");
            owners[shape.SourceFragment!] = record.Alias;
            launches[shape.SourceFragment!] = new CursorLaunch(record.Column, CursorValues.TypeOf(record.Type)!, record.Value);
        }
        return launches;
    }

    /// <summary>
    /// Keeps the cursors a run that succeeded as a whole reported, and says what became of each. A cursor that cannot be kept (a sink or
    /// a source the plan does not give it, a value out of the rule, a store that refuses) advances nothing and is told by name; the run
    /// stays what it was.
    /// </summary>
    public IReadOnlyList<CursorOutcome> Commit(string runId, string pipeline, DistributionPlan plan, RunResult result)
    {
        var reported = result.Reports.Values.Where(r => r.Cursors is not null)
            .SelectMany(r => r.Cursors!).OrderBy(c => c.Key, StringComparer.Ordinal).ToList();
        var outcomes = new List<CursorOutcome>();
        var ready = new List<(string Alias, CursorReport Report, Shape Shape)>();
        foreach (var (alias, report) in reported)
        {
            var shape = Shape.Of(plan, alias);
            if (shape is null) outcomes.Add(Not(alias, "no branch of that name writes in the pipeline."));
            else if (shape.Refusal is { } unserved) outcomes.Add(Not(alias, unserved));
            else if (CursorValues.Refusal(report.Type, report.Value) is { } bad) outcomes.Add(Not(alias, bad));
            else ready.Add((alias, report, shape));
        }
        foreach (var group in ready.GroupBy(r => r.Shape.SourceFragment))
        {
            if (group.Count() > 1)
            {
                outcomes.AddRange(group.Select(r => Not(r.Alias, $"{string.Join(" and ", group.Select(g => $"'{g.Alias}'"))} track a cursor from {group.Key}, which takes one.")));
                continue;
            }
            var (alias, report, shape) = group.Single();
            var record = new CursorRecord(
                pipeline, alias, report.Column, CursorValues.TypeOf(report.Type)!, report.Value, BasisOf(shape), runId, DateTimeOffset.UtcNow, null);
            try
            {
                _store.Save(pipeline, alias, JsonSerializer.Serialize(record, Json));
                lock (_lock) _cursors[(pipeline, alias)] = record;
                outcomes.Add(new CursorOutcome(alias, true, report.Value, null));
            }
            catch (StoreUnavailableException ex)
            {
                _logger.LogError(ex, "The cursor of {Pipeline}/{Alias} could not be kept", pipeline, alias);
                outcomes.Add(Not(alias, "the coordinator's store refused the write, so the cursor did not advance."));
            }
        }
        return outcomes.OrderBy(o => o.Alias, StringComparer.Ordinal).ToList();

        static CursorOutcome Not(string alias, string reason) => new(alias, false, null, $"the cursor of '{alias}' was not kept: {reason}");
    }

    /// <summary>Forgets a cursor: the next run starts from the default of its source's token. False when there was none.</summary>
    public bool Reset(string pipeline, string alias)
    {
        lock (_lock)
        {
            if (!_cursors.ContainsKey((pipeline, alias))) return false;
            _store.Delete(pipeline, alias);
            _cursors.Remove((pipeline, alias));
            return true;
        }
    }

    /// <summary>
    /// Sets a value. A cursor that exists keeps its column, its basis and its type: a value is substituted as text into a query written
    /// for that type, so another type is refused until the cursor is reset. One that does not exist is created without a column or a
    /// basis, and the first run that reports it adopts them. Throws <see cref="ConflictException"/> when the type or the value is out of
    /// the rule.
    /// </summary>
    public CursorRecord Set(string pipeline, string alias, string? type, string value, string by)
    {
        lock (_lock)
        {
            _cursors.TryGetValue((pipeline, alias), out var existing);
            var kind = CursorValues.TypeOf(type ?? existing?.Type)
                ?? throw new ConflictException(type is null ? "Name the type of the value: integer, datetime or string." : $"The type '{type}' is none of integer, datetime, string.");
            if (existing is not null && kind != existing.Type)
                throw new ConflictException($"The cursor of '{alias}' is kept as {existing.Type}, and a value of type {kind} would be written into a query made for it: reset the cursor first.");
            if (CursorValues.Refusal(kind, value) is { } bad) throw new ConflictException($"The value is refused: {bad}");
            var record = new CursorRecord(pipeline, alias, existing?.Column ?? "", kind, value, existing?.Basis, existing?.RunId, DateTimeOffset.UtcNow, by);
            _store.Save(pipeline, alias, JsonSerializer.Serialize(record, Json));
            _cursors[(pipeline, alias)] = record;
            return record;
        }
    }

    private IReadOnlyList<CursorBasis> BasisOf(Shape shape) =>
    [
        new("sink", $"{shape.SinkNode}/{shape.SinkBrick}", _bricks.Find($"{shape.SinkNode}/{shape.SinkBrick}")?.Version ?? ""),
        new("source", $"{shape.SourceNode}/{shape.SourceBrick}", _bricks.Find($"{shape.SourceNode}/{shape.SourceBrick}")?.Version ?? ""),
    ];

    private static string? Changed(CursorRecord record, IReadOnlyList<CursorBasis> kept, IReadOnlyList<CursorBasis> current)
    {
        foreach (var brick in kept)
        {
            var same = current.FirstOrDefault(c => c.Role == brick.Role && c.Brick == brick.Brick);
            if (same is null)
                return $"The {brick.Role} brick '{brick.Brick}' is no longer the one the cursor of '{record.Alias}' was kept with: reset the cursor or set a value.";
            if (same.Version != brick.Version)
                return $"The {brick.Role} brick '{brick.Brick}' changed since the cursor of '{record.Alias}' was kept (version {brick.Version} → {same.Version}): reset the cursor or set a value.";
        }
        return current.Count != kept.Count
            ? $"The source of the cursor of '{record.Alias}' is no longer the one it was kept with: reset the cursor or set a value."
            : null;
    }

    /// <summary>
    /// What the plan makes of a sink alias: the sink unit, the one source unit upstream of it through the pipeline's dependencies, and
    /// the fragments holding them; or why it cannot be served.
    /// </summary>
    private sealed record Shape(
        string? SinkFragment, string? SinkNode, string? SinkBrick, string? SourceFragment, string? SourceNode, string? SourceBrick, string? Refusal)
    {
        public static Shape? Of(DistributionPlan plan, string alias)
        {
            var units = plan.Fragments.Where(f => f.Composition is not null)
                .SelectMany(f => f.Composition!.Select(u => (Fragment: f, Unit: u)))
                .Where(x => x.Unit.Kind is FragmentUnitKind.Source or FragmentUnitKind.SourceSend or FragmentUnitKind.Sink)
                .GroupBy(x => x.Unit.Key, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
            if (!units.TryGetValue(alias, out var sink) || sink.Unit.Kind != FragmentUnitKind.Sink) return null;

            var depends = plan.Units.ToDictionary(u => u.Id, u => u.DependsOn, StringComparer.Ordinal);
            var seen = new HashSet<string>(StringComparer.Ordinal) { alias };
            var queue = new Queue<string>(depends.GetValueOrDefault(alias) ?? []);
            var sources = new List<(PlannedFragment Fragment, FragmentUnit Unit)>();
            while (queue.Count > 0)
            {
                var next = queue.Dequeue();
                if (!seen.Add(next)) continue;
                if (units.TryGetValue(next, out var found) && found.Unit.Kind is FragmentUnitKind.Source or FragmentUnitKind.SourceSend)
                    sources.Add((found.Fragment, found.Unit));
                foreach (var d in depends.GetValueOrDefault(next) ?? []) queue.Enqueue(d);
            }
            if (sources.Count != 1)
                return new Shape(sink.Fragment.Name, sink.Fragment.Node, sink.Unit.Brick, null, null, null,
                    sources.Count == 0 ? "no source brick feeds it." : $"{sources.Count} source bricks feed it ({string.Join(", ", sources.Select(s => $"'{s.Unit.Key}'"))}) and a cursor filters one.");
            var source = sources[0];
            return new Shape(sink.Fragment.Name, sink.Fragment.Node, sink.Unit.Brick, source.Fragment.Name, source.Fragment.Node, source.Unit.Brick, null);
        }
    }
}
