using System.Text.Json;
using System.Text.Json.Serialization;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What the coordinator grants a client of the trusted IdP, named by the <c>sub</c> of its tokens.
/// <see cref="Group"/> is the one TransportR group it belongs to; <see cref="Node"/> is the only node
/// name it may announce itself as. Its secret is the IdP's business, never stored here.
/// </summary>
public sealed record Identity(string ClientId, string DisplayName, string Group, string? Node, bool Enabled = true);

/// <summary>One change to the rights, and who made it (the email of the person signed in), when known.</summary>
public sealed record AuditEntry(DateTimeOffset At, string Change, string? By = null);

public sealed record RightsDocument(
    List<Identity> Identities, Dictionary<string, List<string>> Matrix, List<AuditEntry>? Audit = null);

/// <summary>
/// Who may send what to whom, in one editable document, <c>rights.json</c> in the state directory,
/// started from <see cref="CoordinatorServerOptions.RightsSeed"/>:
/// <list type="bullet">
/// <item><b>who</b>: the clients of the trusted IdP this coordinator grants anything to; the group a
///   node belongs to is its identity's, never one it declares or its token carries;</item>
/// <item><b>to whom</b>: the flow matrix between groups, read live by TransportR's hub on every
///   transfer and by the planner on every plan.</item>
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

    public RightsStore(CoordinatorServerOptions options)
    {
        _path = Path.Combine(options.StateDir, "rights.json");
        // A new state directory starts from the seed, or from no identity and an empty matrix: nobody
        // may send anything until an administrator says so.
        var source = File.Exists(_path) ? _path : options.RightsSeed;
        _doc = source is null
            ? new RightsDocument([], [])
            : JsonSerializer.Deserialize<RightsDocument>(File.ReadAllText(source), Json)
              ?? throw new InvalidOperationException($"{source}: empty rights document.");
        _doc = _doc with { Audit = _doc.Audit ?? [] };
        if (source != _path) Save(source is null ? "Rights started empty" : $"Rights started from {Path.GetFileName(source)}");
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

    public void SetMatrix(Dictionary<string, List<string>> matrix, string? by = null)
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
            Save(changes.Count > 0 ? "Flow matrix: " + string.Join(", ", changes) : "Flow matrix saved, unchanged", by);
        }
    }

    public void AddIdentity(Identity identity, string? by = null)
    {
        lock (_lock)
        {
            if (_doc.Identities.Any(i => i.ClientId == identity.ClientId)) throw new ConflictException($"'{identity.ClientId}' already exists.");
            _doc.Identities.Add(identity);
            Save($"Identity {identity.ClientId} added (group {identity.Group}{(identity.Node is null ? "" : $", node {identity.Node}")})", by);
        }
    }

    public void UpdateIdentity(string clientId, string displayName, string group, string? node, bool enabled, string? by = null)
    {
        lock (_lock)
        {
            var index = _doc.Identities.FindIndex(i => i.ClientId == clientId);
            if (index < 0) throw new ConflictException($"'{clientId}' does not exist.");
            var before = _doc.Identities[index];
            var after = before with { DisplayName = displayName, Group = group, Node = string.IsNullOrWhiteSpace(node) ? null : node, Enabled = enabled };
            _doc.Identities[index] = after;
            var changes = new List<string>();
            if (before.Group != after.Group) changes.Add($"group {before.Group} → {after.Group}");
            if (before.Node != after.Node) changes.Add($"node {before.Node ?? "none"} → {after.Node ?? "none"}");
            if (before.Enabled != after.Enabled) changes.Add(after.Enabled ? "enabled" : "disabled");
            if (before.DisplayName != after.DisplayName) changes.Add("renamed");
            Save($"Identity {clientId}: " + (changes.Count > 0 ? string.Join(", ", changes) : "unchanged"), by);
        }
    }

    public void RemoveIdentity(string clientId, string? by = null)
    {
        lock (_lock)
        {
            if (_doc.Identities.RemoveAll(i => i.ClientId == clientId) == 0) throw new ConflictException($"'{clientId}' does not exist.");
            Save($"Identity {clientId} removed", by);
        }
    }

    private void Save(string change, string? by = null)
    {
        _doc.Audit!.Insert(0, new AuditEntry(DateTimeOffset.UtcNow, change, by));
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
