using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Lab.Contracts;
using TransportR.Interfaces;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Lab.Coordinator;

/// <summary>
/// A principal the embedded identity provider issues tokens to. <see cref="Group"/> is the one
/// TransportR group its tokens carry (<c>transportr:group:&lt;group&gt;</c>); <see cref="Node"/> is
/// the only node name it may announce itself as.
/// </summary>
public sealed record Identity(string ClientId, string DisplayName, string Secret, string Group, string? Node, bool Enabled = true);

public sealed record AuditEntry(DateTimeOffset At, string Change);

public sealed record RightsDocument(
    List<Identity> Identities, Dictionary<string, List<string>> Matrix, Dictionary<string, List<string>> BrickPolicies,
    List<AuditEntry>? Audit = null);

/// <summary>
/// Who may send what to whom, in one editable document under <c>.state/rights.json</c>, started
/// from <c>rights-seed.json</c>:
/// <list type="bullet">
/// <item><b>who</b>: the identities of the embedded IDP; the group a node belongs to is the one
///   its token carries, never the one it declares;</item>
/// <item><b>to whom</b>: the flow matrix between groups, read live by TransportR's hub on every
///   transfer and by the planner on every plan;</item>
/// <item><b>what</b>: per source brick, the groups its rows may be sent to as they leave their
///   node, checked on every plan (the hub sees groups, not bricks).</item>
/// </list>
/// </summary>
public sealed class RightsStore
{
    private const int AuditKept = 200;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    private readonly string _path;
    private readonly object _lock = new();
    private RightsDocument _doc;

    public event Action? Changed;

    public RightsStore(LabOptions options)
    {
        _path = Path.Combine(options.StateDir, "rights.json");
        var source = File.Exists(_path) ? _path : Path.Combine(options.LabRoot, "rights-seed.json");
        _doc = JsonSerializer.Deserialize<RightsDocument>(File.ReadAllText(source), Json)
               ?? throw new InvalidOperationException($"{source}: empty rights document.");
        _doc = _doc with { Audit = _doc.Audit ?? [] };
        if (source != _path) Save("Rights started from rights-seed.json");
    }

    public RightsDocument Snapshot()
    {
        lock (_lock) return JsonSerializer.Deserialize<RightsDocument>(JsonSerializer.Serialize(_doc, Json), Json)!;
    }

    public Identity? Find(string? clientId)
    {
        lock (_lock) return _doc.Identities.FirstOrDefault(i => i.ClientId == clientId);
    }

    public IReadOnlyList<string> Groups()
    {
        lock (_lock)
        {
            return _doc.Identities.Select(i => i.Group).Concat(_doc.Matrix.Keys).Concat(_doc.Matrix.Values.SelectMany(v => v))
                .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        }
    }

    public bool CanSend(string from, string to)
    {
        lock (_lock) return _doc.Matrix.TryGetValue(from, out var targets) && (targets.Contains("*") || targets.Contains(to, StringComparer.OrdinalIgnoreCase));
    }

    public void SetMatrix(Dictionary<string, List<string>> matrix)
    {
        lock (_lock)
        {
            var changes = new List<string>();
            foreach (var group in matrix.Keys.Union(_doc.Matrix.Keys).Order(StringComparer.Ordinal))
            {
                var before = _doc.Matrix.GetValueOrDefault(group) ?? [];
                var after = matrix.GetValueOrDefault(group) ?? [];
                foreach (var t in after.Except(before)) changes.Add($"{group} → {t} allowed");
                foreach (var t in before.Except(after)) changes.Add($"{group} → {t} revoked");
            }
            _doc.Matrix.Clear();
            foreach (var (group, targets) in matrix) _doc.Matrix[group] = targets.Distinct().Order(StringComparer.Ordinal).ToList();
            Save(changes.Count > 0 ? "Flow matrix: " + string.Join(", ", changes) : "Flow matrix saved, unchanged");
        }
    }

    public void SetBrickPolicy(string brickKey, List<string>? groups)
    {
        lock (_lock)
        {
            if (groups is null) _doc.BrickPolicies.Remove(brickKey);
            else _doc.BrickPolicies[brickKey] = groups.Distinct().Order(StringComparer.Ordinal).ToList();
            Save(groups is null ? $"{brickKey}: may go wherever the matrix allows" : $"{brickKey}: may leave its node only for {(groups.Count == 0 ? "no group" : string.Join(", ", groups))}");
        }
    }

    public void AddIdentity(Identity identity)
    {
        lock (_lock)
        {
            if (_doc.Identities.Any(i => i.ClientId == identity.ClientId)) throw new LabConflictException($"'{identity.ClientId}' already exists.");
            if (string.IsNullOrWhiteSpace(identity.Secret) || identity.Secret.Length < 12) throw new LabConflictException("A secret has at least 12 characters.");
            _doc.Identities.Add(identity);
            Save($"Identity {identity.ClientId} added (group {identity.Group}{(identity.Node is null ? "" : $", node {identity.Node}")})");
        }
    }

    public void UpdateIdentity(string clientId, string displayName, string group, string? node, bool enabled)
    {
        lock (_lock)
        {
            var index = _doc.Identities.FindIndex(i => i.ClientId == clientId);
            if (index < 0) throw new LabConflictException($"'{clientId}' does not exist.");
            var before = _doc.Identities[index];
            var after = before with { DisplayName = displayName, Group = group, Node = string.IsNullOrWhiteSpace(node) ? null : node, Enabled = enabled };
            _doc.Identities[index] = after;
            var changes = new List<string>();
            if (before.Group != after.Group) changes.Add($"group {before.Group} → {after.Group}");
            if (before.Node != after.Node) changes.Add($"node {before.Node ?? "none"} → {after.Node ?? "none"}");
            if (before.Enabled != after.Enabled) changes.Add(after.Enabled ? "enabled" : "disabled");
            if (before.DisplayName != after.DisplayName) changes.Add("renamed");
            Save($"Identity {clientId}: " + (changes.Count > 0 ? string.Join(", ", changes) : "unchanged"));
        }
    }

    public void RemoveIdentity(string clientId)
    {
        lock (_lock)
        {
            if (_doc.Identities.RemoveAll(i => i.ClientId == clientId) == 0) throw new LabConflictException($"'{clientId}' does not exist.");
            Save($"Identity {clientId} removed");
        }
    }

    /// <summary>
    /// The plan's edges that carry a source brick's rows off its node to a group its policy does
    /// not allow. A brick is found by content in the logical job; an edge's consumer alias is always
    /// the alias of the branch whose rows it carries.
    /// </summary>
    public IReadOnlyList<string> PolicyViolations(LabPlan plan, string yaml, BrickCatalog bricks)
    {
        Dictionary<string, List<string>> policies;
        lock (_lock) policies = _doc.BrickPolicies.ToDictionary(kv => kv.Key, kv => kv.Value.ToList());
        if (policies.Count == 0) return [];

        var brickOf = new Dictionary<string, BrickView>(StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
                if (bricks.Match(branch) is { } brick) brickOf[alias] = brick;
        }
        catch (Exception) { return []; }

        var violations = new List<string>();
        foreach (var edge in plan.Edges)
        {
            if (!brickOf.TryGetValue(edge.ConsumerAlias, out var brick) || !policies.TryGetValue(brick.Key, out var allowed)) continue;
            if (!allowed.Contains(edge.ConsumerGroup, StringComparer.OrdinalIgnoreCase))
                violations.Add($"{brick.Key} ('{edge.ConsumerAlias}') may leave {brick.Node} only for " +
                               $"{(allowed.Count == 0 ? "no group" : string.Join(", ", allowed))}; this plan sends it to {edge.ConsumerFragment} (group {edge.ConsumerGroup}).");
        }
        return violations.Distinct().ToList();
    }

    public LabPlan WithPolicies(LabPlan plan, string yaml, BrickCatalog bricks)
    {
        var violations = PolicyViolations(plan, yaml, bricks);
        return violations.Count == 0 ? plan : plan with { Errors = [.. plan.Errors, .. violations] };
    }

    private void Save(string change)
    {
        _doc.Audit!.Insert(0, new AuditEntry(DateTimeOffset.UtcNow, change));
        if (_doc.Audit.Count > AuditKept) _doc.Audit.RemoveRange(AuditKept, _doc.Audit.Count - AuditKept);
        Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
        File.WriteAllText(_path, JsonSerializer.Serialize(_doc, Json));
        Changed?.Invoke();
    }
}

/// <summary>
/// TransportR's flow control, answered from the rights document as it is now: an edit of the
/// matrix applies to the next transfer the hub opens, with no restart.
/// </summary>
public sealed class RightsFlowControl(RightsStore rights) : IFlowControlService
{
    public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) =>
        sourceGroups.Count > 0 && targetGroups.Count > 0 &&
        targetGroups.All(target => sourceGroups.Any(source => rights.CanSend(source, target)));
}
