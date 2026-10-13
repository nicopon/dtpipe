using System.Text.Json;
using System.Text.Json.Serialization;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What the coordinator grants a client of the trusted IdP, named by the <c>sub</c> of its tokens.
/// <see cref="Name"/> is the one node name it may announce itself as, and belongs to the rights: a host
/// only proposes one. <see cref="Groups"/> are the TransportR groups it belongs to. Its secret is the
/// IdP's business, never stored here.
/// </summary>
public sealed record Identity(string ClientId, string Name, string Description, IReadOnlyList<string> Groups, bool Enabled = true);

/// <summary>One change to the rights, and who made it (the email of the person signed in), when known.</summary>
public sealed record AuditEntry(DateTimeOffset At, string Change, string? By = null);

/// <summary>The rights document: the identities, the flow matrix between groups, and the changes made to them.</summary>
public sealed record RightsDocument(
    List<Identity> Identities, Dictionary<string, List<string>> Matrix, List<AuditEntry>? Audit = null);

/// <summary>
/// Who may send what to whom, in one editable document, <c>rights.json</c> in the state directory,
/// started from <see cref="CoordinatorServerOptions.RightsSeed"/>:
/// <list type="bullet">
/// <item><b>who</b>: the clients of the trusted IdP this coordinator grants anything to; the name and
///   the groups of a node are its identity's, never ones it declares or its token carries;</item>
/// <item><b>to whom</b>: the flow matrix between groups, read live by TransportR's hub on every
///   transfer and by the planner on every plan. A node sends to another only if every group of the
///   sender may send to every group of the receiver.</item>
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
        _doc = _doc with { Identities = [.. _doc.Identities.Select(Normalized)], Audit = _doc.Audit ?? [] };
        CheckUnique(_doc.Identities);
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

    /// <summary>
    /// The subject node <paramref name="node"/> connects under: the client id of the one identity that
    /// announces itself with that name (a node name belongs to one identity). Refused when no identity
    /// carries the name any more, or when the identity is disabled, as nothing then says who may host the
    /// node's fragments.
    /// </summary>
    public string SubjectOf(string node)
    {
        lock (_lock) return ActiveHostLocked(node, "who may host its fragments").ClientId;
    }

    /// <summary>
    /// The groups the rights give the node named <paramref name="node"/> now, which its replicas' data plane clients must hold.
    /// Refused for a node with no identity or a disabled one, as <see cref="SubjectOf"/> is.
    /// </summary>
    public IReadOnlyList<string> GroupsOf(string node)
    {
        lock (_lock) return ActiveHostLocked(node, "which groups it holds").Groups;
    }

    /// <summary>The groups the rights give the node named <paramref name="node"/> now, or null when no identity carries the name or it is disabled.</summary>
    public IReadOnlyList<string>? ActiveGroupsOf(string node)
    {
        lock (_lock) return IdentityNamedLocked(node) is { Enabled: true } identity ? identity.Groups : null;
    }

    private Identity? IdentityNamedLocked(string node) => _doc.Identities.FirstOrDefault(i => i.Name == node);

    /// <summary>The identity that can host <paramref name="node"/> now: the one that carries the name, if it is enabled.</summary>
    private Identity ActiveHostLocked(string node, string nothingSays)
    {
        var identity = IdentityNamedLocked(node)
            ?? throw new ConflictException($"Node '{node}' has no identity in the rights: nothing says {nothingSays}.");
        return identity.Enabled
            ? identity
            : throw new ConflictException($"Node '{node}' is disabled: its identity may not connect, so no run is placed on it.");
    }

    public IReadOnlyList<string> Groups()
    {
        lock (_lock)
        {
            return _doc.Identities.SelectMany(i => i.Groups).Concat(_doc.Matrix.Keys).Concat(_doc.Matrix.Values.SelectMany(v => v))
                .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        }
    }

    /// <summary>Whether every group of the sender may send to every group of the receiver; false when either has none.</summary>
    public bool CanSend(IReadOnlySet<string> from, IReadOnlySet<string> to)
    {
        lock (_lock) return from.Count > 0 && to.Count > 0 && from.All(f => to.All(t => AllowedLocked(f, t)));
    }

    private bool AllowedLocked(string from, string to) =>
        _doc.Matrix.TryGetValue(from, out var targets) && (targets.Contains("*") || targets.Contains(to, StringComparer.OrdinalIgnoreCase));

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
            var added = Normalized(identity);
            CheckUnique([.. _doc.Identities, added]);
            _doc.Identities.Add(added);
            Save($"Identity {added.ClientId} added (node {added.Name}, groups {string.Join(", ", added.Groups)})", by);
        }
    }

    /// <summary>
    /// Edits an identity. An argument that is null is left as the identity has it: an edit says what it changes, and a field it
    /// leaves out is neither emptied, renamed nor enabled by the omission. The identity is read and written under one lock.
    /// </summary>
    public void UpdateIdentity(string clientId, string? name, string? description, IReadOnlyList<string>? groups, bool? enabled, string? by = null)
    {
        lock (_lock)
        {
            var index = _doc.Identities.FindIndex(i => i.ClientId == clientId);
            if (index < 0) throw new ConflictException($"'{clientId}' does not exist.");
            var before = _doc.Identities[index];
            var after = Normalized(before with
            {
                Name = name ?? before.Name, Description = description ?? before.Description,
                Groups = groups ?? before.Groups, Enabled = enabled ?? before.Enabled,
            });
            CheckUnique(_doc.Identities.Select((i, at) => at == index ? after : i).ToList());
            _doc.Identities[index] = after;
            var changes = new List<string>();
            if (!before.Groups.SequenceEqual(after.Groups)) changes.Add($"groups {string.Join("+", before.Groups)} → {string.Join("+", after.Groups)}");
            if (before.Name != after.Name) changes.Add($"node {before.Name} → {after.Name}");
            if (before.Enabled != after.Enabled) changes.Add(after.Enabled ? "enabled" : "disabled");
            if (before.Description != after.Description) changes.Add("described again");
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

    /// <summary>The identity with its name and groups trimmed, its groups sorted and distinct; refused when its name or groups are unusable.</summary>
    private static Identity Normalized(Identity identity)
    {
        var name = identity.Name?.Trim() ?? "";
        if (!PipelineLibrary.IsValidId(name)) throw new ConflictException($"'{name}' is not a node name: lowercase letters, digits and '-'.");
        var groups = (identity.Groups ?? []).Select(g => g.Trim()).Where(g => g.Length > 0).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        if (groups.Count == 0) throw new ConflictException($"'{identity.ClientId}' belongs to no group: an identity belongs to at least one.");
        return identity with { Name = name, Description = identity.Description ?? "", Groups = groups };
    }

    private static void CheckUnique(IReadOnlyList<Identity> identities)
    {
        foreach (var duplicate in identities.GroupBy(i => i.Name, StringComparer.Ordinal).Where(g => g.Count() > 1))
            throw new ConflictException($"'{duplicate.Key}' is the node name of {string.Join(" and ", duplicate.Select(i => i.ClientId))}: a node name belongs to one identity.");
        foreach (var duplicate in identities.GroupBy(i => i.ClientId, StringComparer.Ordinal).Where(g => g.Count() > 1))
            throw new ConflictException($"'{duplicate.Key}' is the client id of more than one identity.");
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
    public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) => rights.CanSend(sourceGroups, targetGroups);
}
