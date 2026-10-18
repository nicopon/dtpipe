using System.Text.Json;
using System.Text.Json.Serialization;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What the coordinator grants a client of the trusted IdP, named by the <c>sub</c> of its tokens.
/// <see cref="Name"/> is the one node name it may announce itself as, and belongs to the rights: a host
/// only proposes one. <see cref="Groups"/> are the TransportR groups it belongs to. Its secret is the
/// IdP's business, never stored here. <see cref="Kind"/> says what the coordinator grants it: a node (the default) hosts fragments
/// and belongs to groups; a monitor only reads <c>/health</c>, belongs to none, and its name is a label that no node may carry; a
/// launcher starts the runs of <see cref="Pipelines"/> and reads them, belongs to no group, and its name is a label too.
/// </summary>
public sealed record Identity(
    string ClientId, string Name, string Description, IReadOnlyList<string> Groups, bool Enabled = true, string Kind = IdentityKinds.Node,
    IReadOnlyList<string>? Pipelines = null)
{
    public bool IsNode => Kind == IdentityKinds.Node;

    /// <summary>The identity is an enabled launcher of <paramref name="pipelineId"/>; a list that names none lets it launch none.</summary>
    public bool Launches(string pipelineId) => Enabled && Kind == IdentityKinds.Launcher && Pipelines is { } list && list.Contains(pipelineId);
}

/// <summary>What an identity of the trusted IdP is granted: the sort of service it is. A node is the only sort the node channel and the data plane know.</summary>
public static class IdentityKinds
{
    /// <summary>A host of fragments: it announces on the node channel and belongs to groups.</summary>
    public const string Node = "node";

    /// <summary>A supervisor: it reads <c>/health</c> and nothing else, with no group and no node.</summary>
    public const string Monitor = "monitor";

    /// <summary>A scheduler or a CI: it starts the runs of the pipelines its identity lists and reads them, with no group and no node.</summary>
    public const string Launcher = "launcher";

    public static readonly IReadOnlyList<string> All = [Node, Monitor, Launcher];
}

/// <summary>One change to the rights, and who made it (the email of the person signed in), when known.</summary>
public sealed record AuditEntry(DateTimeOffset At, string Change, string? By = null);

/// <summary>The rights document: the identities, the flow matrix between groups, and the changes made to them.</summary>
public sealed record RightsDocument(
    List<Identity> Identities, Dictionary<string, List<string>> Matrix, List<AuditEntry>? Audit = null);

/// <summary>
/// Who may send what to whom, in one editable document kept in an <see cref="IRightsStore"/>, started from
/// <see cref="CoordinatorServerOptions.RightsSeed"/> when the store holds none:
/// <list type="bullet">
/// <item><b>who</b>: the clients of the trusted IdP this coordinator grants anything to; the name and
///   the groups of a node are its identity's, never ones it declares or its token carries;</item>
/// <item><b>to whom</b>: the flow matrix between groups, read live by TransportR's hub on every
///   transfer and by the planner on every plan. A node sends to another only if every group of the
///   sender may send to every group of the receiver.</item>
/// </list>
/// The document is read once, at start, and memory is the truth after it: a reader never waits for the store. A change is made on a
/// copy, written, and published only when the store kept it, so a store that refuses leaves the rights as they were. A write that
/// finds the store at another version than the one this store last read (another writer, or an earlier write acknowledged late)
/// reads the document again and refuses that one change.
/// </summary>
public sealed class RightsStore
{
    private const int AuditKept = 200;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    private sealed record State(RightsDocument Doc, long Version);

    private readonly IRightsStore _store;
    private readonly object _write = new();
    // Replaced whole, never edited: a reader takes the reference once and needs no lock.
    private volatile State _state;

    public event Action? Changed;

    public RightsStore(IRightsStore store, CoordinatorServerOptions options)
    {
        _store = store;
        // A new store starts from the seed, or from no identity and an empty matrix: nobody
        // may send anything until an administrator says so.
        if (store.Load() is not { } stored)
        {
            var seed = options.RightsSeed;
            var doc = seed is null ? new RightsDocument([], []) : Read(File.ReadAllText(seed), seed);
            doc = Record(Prepared(doc), seed is null ? "Rights started empty" : $"Rights started from {Path.GetFileName(seed)}", null);
            if (store.Create(Serialize(doc)))
            {
                _state = new State(doc, 1);
                return;
            }
            // A start that crossed this one wrote first: its document is the rights.
            stored = store.Load() ?? throw new InvalidOperationException("The rights document disappeared while the coordinator started.");
        }
        _state = new State(Prepared(Read(stored.Document, "the rights store")), stored.Version);
    }

    public RightsDocument Snapshot() => Copy(_state.Doc);

    public Identity? Find(string? clientId) => _state.Doc.Identities.FirstOrDefault(i => i.ClientId == clientId);

    /// <summary>
    /// The subject node <paramref name="node"/> connects under: the client id of the one identity that
    /// announces itself with that name (a node name belongs to one identity). Refused when no identity
    /// carries the name any more, or when the identity is disabled, as nothing then says who may host the
    /// node's fragments.
    /// </summary>
    public string SubjectOf(string node) => ActiveHost(node, "who may host its fragments").ClientId;

    /// <summary>
    /// The groups the rights give the node named <paramref name="node"/> now, which its replicas' data plane clients must hold.
    /// Refused for a node with no identity or a disabled one, as <see cref="SubjectOf"/> is.
    /// </summary>
    public IReadOnlyList<string> GroupsOf(string node) => ActiveHost(node, "which groups it holds").Groups;

    /// <summary>The identity that carries the node name <paramref name="node"/>, enabled or not; a name that a monitor carries is no node's.</summary>
    public Identity? NodeIdentity(string node) => IdentityNamed(_state.Doc, node);

    private static Identity? IdentityNamed(RightsDocument doc, string node) => doc.Identities.FirstOrDefault(i => i.IsNode && i.Name == node);

    /// <summary>The identity that can host <paramref name="node"/> now: the one that carries the name, if it is enabled.</summary>
    private Identity ActiveHost(string node, string nothingSays)
    {
        var identity = IdentityNamed(_state.Doc, node)
            ?? throw new ConflictException($"Node '{node}' has no identity in the rights: nothing says {nothingSays}.");
        return identity.Enabled
            ? identity
            : throw new ConflictException($"Node '{node}' is disabled: its identity may not connect, so no run is placed on it.");
    }

    public IReadOnlyList<string> Groups()
    {
        var doc = _state.Doc;
        return doc.Identities.SelectMany(i => i.Groups).Concat(doc.Matrix.Keys).Concat(doc.Matrix.Values.SelectMany(v => v))
            .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
    }

    /// <summary>Whether every group of the sender may send to every group of the receiver; false when either has none.</summary>
    public bool CanSend(IReadOnlySet<string> from, IReadOnlySet<string> to)
    {
        var doc = _state.Doc;
        return from.Count > 0 && to.Count > 0 && from.All(f => to.All(t => Allowed(doc, f, t)));
    }

    private static bool Allowed(RightsDocument doc, string from, string to) =>
        doc.Matrix.TryGetValue(from, out var targets) && (targets.Contains("*") || targets.Contains(to, StringComparer.OrdinalIgnoreCase));

    public void SetMatrix(Dictionary<string, List<string>> matrix, string? by = null) => Edit(by, doc =>
    {
        var changes = new List<string>();
        foreach (var group in matrix.Keys.Union(doc.Matrix.Keys).Order(StringComparer.Ordinal))
        {
            var before = doc.Matrix.GetValueOrDefault(group) ?? [];
            var after = matrix.GetValueOrDefault(group) ?? [];
            foreach (var t in after.Except(before)) changes.Add($"{group} → {t} allowed");
            foreach (var t in before.Except(after)) changes.Add($"{group} → {t} revoked");
        }
        doc.Matrix.Clear();
        foreach (var (group, targets) in matrix) doc.Matrix[group] = targets.Distinct().Order(StringComparer.Ordinal).ToList();
        return changes.Count > 0 ? "Flow matrix: " + string.Join(", ", changes) : "Flow matrix saved, unchanged";
    });

    public void AddIdentity(Identity identity, string? by = null) => Edit(by, doc =>
    {
        if (doc.Identities.Any(i => i.ClientId == identity.ClientId)) throw new ConflictException($"'{identity.ClientId}' already exists.");
        var added = Normalized(identity);
        CheckUnique([.. doc.Identities, added]);
        doc.Identities.Add(added);
        return added.IsNode
            ? $"Identity {added.ClientId} added (node {added.Name}, groups {string.Join(", ", added.Groups)})"
            : added.Kind == IdentityKinds.Launcher
                ? $"Identity {added.ClientId} added ({added.Kind} {added.Name}, pipelines {string.Join(", ", added.Pipelines!)})"
                : $"Identity {added.ClientId} added ({added.Kind} {added.Name})";
    });

    /// <summary>
    /// Edits an identity. An argument that is null is left as the identity has it: an edit says what it changes, and a field it
    /// leaves out is neither emptied, renamed nor enabled by the omission. The identity is read and written as one change.
    /// </summary>
    public void UpdateIdentity(string clientId, string? name, string? description, IReadOnlyList<string>? groups, bool? enabled, string? by = null, IReadOnlyList<string>? pipelines = null) =>
        Edit(by, doc =>
        {
            var index = doc.Identities.FindIndex(i => i.ClientId == clientId);
            if (index < 0) throw new ConflictException($"'{clientId}' does not exist.");
            var before = doc.Identities[index];
            var after = Normalized(before with
            {
                Name = name ?? before.Name, Description = description ?? before.Description,
                Groups = groups ?? before.Groups, Enabled = enabled ?? before.Enabled, Pipelines = pipelines ?? before.Pipelines,
            });
            CheckUnique(doc.Identities.Select((i, at) => at == index ? after : i).ToList());
            doc.Identities[index] = after;
            var changes = new List<string>();
            if (!before.Groups.SequenceEqual(after.Groups)) changes.Add($"groups {string.Join("+", before.Groups)} → {string.Join("+", after.Groups)}");
            if (!(before.Pipelines ?? []).SequenceEqual(after.Pipelines!)) changes.Add($"pipelines {string.Join("+", before.Pipelines ?? [])} → {string.Join("+", after.Pipelines!)}");
            if (before.Name != after.Name) changes.Add($"node {before.Name} → {after.Name}");
            if (before.Enabled != after.Enabled) changes.Add(after.Enabled ? "enabled" : "disabled");
            if (before.Description != after.Description) changes.Add("described again");
            return $"Identity {clientId}: " + (changes.Count > 0 ? string.Join(", ", changes) : "unchanged");
        });

    public void RemoveIdentity(string clientId, string? by = null) => Edit(by, doc =>
    {
        if (doc.Identities.RemoveAll(i => i.ClientId == clientId) == 0) throw new ConflictException($"'{clientId}' does not exist.");
        return $"Identity {clientId} removed";
    });

    /// <summary>The identity with its name, groups and pipelines trimmed, sorted and distinct; refused when its name, groups or pipelines are unusable.</summary>
    private static Identity Normalized(Identity identity)
    {
        var kind = string.IsNullOrWhiteSpace(identity.Kind) ? IdentityKinds.Node : identity.Kind.Trim();
        if (!IdentityKinds.All.Contains(kind))
            throw new ConflictException($"'{identity.ClientId}' is of kind '{kind}': the kinds are {string.Join(", ", IdentityKinds.All)}.");
        var name = identity.Name?.Trim() ?? "";
        if (!PipelineLibrary.IsValidId(name)) throw new ConflictException($"'{name}' is not a {(kind == IdentityKinds.Node ? "node name" : "name")}: lowercase letters, digits and '-'.");
        var groups = (identity.Groups ?? []).Select(g => g.Trim()).Where(g => g.Length > 0).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        // Only a node belongs to groups: they are what the data plane admits it with, and a service of another kind is never admitted there.
        if (kind != IdentityKinds.Node) groups = [];
        else if (groups.Count == 0) throw new ConflictException($"'{identity.ClientId}' belongs to no group: an identity belongs to at least one.");
        // Only a launcher lists pipelines: a monitor reads them all, and a list on any other kind would grant nothing.
        var pipelines = (identity.Pipelines ?? []).Select(p => p.Trim()).Where(p => p.Length > 0).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        if (kind != IdentityKinds.Launcher) pipelines = [];
        else if (pipelines.FirstOrDefault(p => !PipelineLibrary.IsValidId(p)) is { } bad)
            throw new ConflictException($"'{bad}' is not a pipeline id: lowercase letters, digits and '-'.");
        return identity with { Name = name, Description = identity.Description ?? "", Groups = groups, Kind = kind, Pipelines = pipelines };
    }

    private static void CheckUnique(IReadOnlyList<Identity> identities)
    {
        foreach (var duplicate in identities.GroupBy(i => i.Name, StringComparer.Ordinal).Where(g => g.Count() > 1))
            throw new ConflictException($"'{duplicate.Key}' is the node name of {string.Join(" and ", duplicate.Select(i => i.ClientId))}: a node name belongs to one identity.");
        foreach (var duplicate in identities.GroupBy(i => i.ClientId, StringComparer.Ordinal).Where(g => g.Count() > 1))
            throw new ConflictException($"'{duplicate.Key}' is the client id of more than one identity.");
    }

    /// <summary>
    /// Applies <paramref name="edit"/>, which changes a copy of the rights and says what it changed, then writes the copy and publishes it.
    /// An edit that is refused, or a store that cannot keep the copy, leaves the rights as they are.
    /// </summary>
    private void Edit(string? by, Func<RightsDocument, string> edit)
    {
        lock (_write)
        {
            var current = _state;
            var next = Copy(current.Doc);
            next = Record(next, edit(next), by);
            if (_store.Replace(Serialize(next), current.Version) is not { } version)
            {
                Reread();
                throw new ConflictException(
                    "The rights changed in the store since this coordinator read them (another writer, or an earlier change that was kept after it reported failing). They have been read again: repeat the change.");
            }
            _state = new State(next, version);
            Changed?.Invoke();
        }
    }

    private void Reread()
    {
        if (_store.Load() is not { } stored) return;
        _state = new State(Prepared(Read(stored.Document, "the rights store")), stored.Version);
        Changed?.Invoke();
    }

    /// <summary>The document with its identities normalized and unique, and an audit list.</summary>
    private static RightsDocument Prepared(RightsDocument doc)
    {
        var prepared = doc with { Identities = [.. doc.Identities.Select(Normalized)], Audit = doc.Audit ?? [] };
        CheckUnique(prepared.Identities);
        return prepared;
    }

    /// <summary>The document with <paramref name="change"/> at the head of its audit, which keeps the latest <see cref="AuditKept"/>.</summary>
    private static RightsDocument Record(RightsDocument doc, string change, string? by)
    {
        doc.Audit!.Insert(0, new AuditEntry(DateTimeOffset.UtcNow, change, by));
        if (doc.Audit.Count > AuditKept) doc.Audit.RemoveRange(AuditKept, doc.Audit.Count - AuditKept);
        return doc;
    }

    private static RightsDocument Read(string text, string source) =>
        JsonSerializer.Deserialize<RightsDocument>(text, Json) ?? throw new InvalidOperationException($"{source}: empty rights document.");

    private static string Serialize(RightsDocument doc) => JsonSerializer.Serialize(doc, Json);

    private static RightsDocument Copy(RightsDocument doc) => Read(Serialize(doc), "the rights");
}

/// <summary>
/// TransportR's flow control, answered from the rights document as it is now: an edit of the
/// matrix applies to the next transfer the hub opens, with no restart.
/// </summary>
public sealed class RightsFlowControl(RightsStore rights) : IFlowControlService
{
    public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) => rights.CanSend(sourceGroups, targetGroups);
}
