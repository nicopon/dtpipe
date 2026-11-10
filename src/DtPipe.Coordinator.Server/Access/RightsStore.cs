using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator.Server.Deployments;
using DtPipe.Coordinator.Server.Library;
using DtPipe.Coordinator.Server.Stores;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server.Access;

/// <summary>
/// What the coordinator grants a client of an issuer it trusts, named by the <c>sub</c> of its tokens.
/// <see cref="Name"/> is the one node name it may announce itself as, and belongs to the rights: a host
/// only proposes one. <see cref="Groups"/> are the TransportR groups it belongs to. Its secret is never stored here: the IdP keeps
/// it, or the rights keep a key's hash in <see cref="Auth"/>. <see cref="Kind"/> says what the coordinator grants it: a node (the default) hosts fragments
/// and belongs to groups; a monitor only reads <c>/health</c>, belongs to none, and its name is a label that no node may carry; a
/// launcher starts the runs of <see cref="Pipelines"/> and reads them, belongs to no group, and its name is a label too; a person
/// signs in to the pages and the studio with the <see cref="Role"/> it holds, belongs to no group, and its name is a label too.
/// <see cref="ClientId"/> is unique among the identities of every kind, so an id written as the author of a change or a run names
/// one identity.
/// </summary>
public sealed record Identity(
    string ClientId, string Name, string Description, IReadOnlyList<string> Groups, bool Enabled = true, string Kind = IdentityKinds.Node,
    IReadOnlyList<string>? Pipelines = null)
{
    public IdentityAuth Auth { get; init; } = IdentityAuth.Idp;

    /// <summary>A person's role, cumulative as <see cref="CoordinatorRoles"/> grants it: <c>admin</c>, <c>designer</c> or <c>reader</c>; none for another kind.</summary>
    public string? Role { get; init; }

    /// <summary>
    /// What a person of the identity provider is found by: the value the provider gives it for <c>Coordinator:Roles:Claim</c> (its email
    /// by default), compared without case, never its <c>sub</c>. None for any other identity: a person verified by a key is found by its id.
    /// </summary>
    public string? Account { get; init; }

    public bool IsNode => Kind == IdentityKinds.Node;

    public bool IsPerson => Kind == IdentityKinds.Person;

    /// <summary>The identity is an enabled launcher of <paramref name="pipelineId"/>; a list that names none lets it launch none.</summary>
    public bool Launches(string pipelineId) => Enabled && Kind == IdentityKinds.Launcher && Pipelines is { } list && list.Contains(pipelineId);
}

/// <summary>What an identity is granted: the sort of service it is, or a person. A node is the only sort the node channel and the data plane know.</summary>
public static class IdentityKinds
{
    /// <summary>A host of fragments: it announces on the node channel and belongs to groups.</summary>
    public const string Node = "node";

    /// <summary>A supervisor: it reads <c>/health</c> and nothing else, with no group and no node.</summary>
    public const string Monitor = "monitor";

    /// <summary>A scheduler or a CI: it starts the runs of the pipelines its identity lists and reads them, with no group and no node.</summary>
    public const string Launcher = "launcher";

    /// <summary>
    /// Someone who signs in to the pages and the studio, with a key or through the IdP (found by its <see cref="Identity.Account"/>),
    /// holding the role its identity gives, with no group and no node.
    /// </summary>
    public const string Person = "person";

    public static readonly IReadOnlyList<string> All = [Node, Monitor, Launcher, Person];

    /// <summary>The kinds an identity verified by a key may be of.</summary>
    public static readonly IReadOnlyList<string> Keyed = [Node, Monitor, Launcher, Person];
}

/// <summary>
/// One change to the rights, and who made it: the email of the person signed in, or the id of an identity with none (a person verified
/// by a key), when known.
/// </summary>
public sealed record AuditEntry(DateTimeOffset At, string Change, string? By = null);

/// <summary>An edge of the flow matrix: <see cref="From"/> may send to <see cref="To"/>.</summary>
public sealed record GroupEdge(string From, string To);

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
    private const int MaxLiveKeys = 2;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    private sealed record State(RightsDocument Doc, long Version);

    private readonly IRightsStore _store;
    private readonly CoordinatorServerOptions _options;
    private readonly IConfiguration? _configuration;
    private readonly object _write = new();
    // Replaced whole, never edited: a reader takes the reference once and needs no lock.
    private volatile State _state;

    public event Action? Changed;

    /// <param name="configuration">Where <c>Coordinator:Roles:Admin</c> is read, which counts as an administrator beside the rights' own.</param>
    public RightsStore(IRightsStore store, CoordinatorServerOptions options, IConfiguration? configuration = null)
    {
        (_store, _options, _configuration) = (store, options, configuration);
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
            .Where(g => g != "*")
            .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
    }

    /// <summary>Whether every group of the sender may send to every group of the receiver; false when either has none.</summary>
    public bool CanSend(IReadOnlySet<string> from, IReadOnlySet<string> to)
    {
        var doc = _state.Doc;
        return from.Count > 0 && to.Count > 0 && from.All(f => to.All(t => Allowed(doc, f, t)));
    }

    private static bool Allowed(RightsDocument doc, string from, string to) =>
        (doc.Matrix.TryGetValue(from, out var targets) && (targets.Contains("*") || targets.Contains(to, StringComparer.OrdinalIgnoreCase)))
        || (to != "*" && doc.Matrix.TryGetValue("*", out var any) && any.Contains(to, StringComparer.OrdinalIgnoreCase));

    public void SetMatrix(Dictionary<string, List<string>> matrix, string? by = null) => Edit(by, doc =>
    {
        if (matrix.TryGetValue("*", out var wildcardTargets) && wildcardTargets.Contains("*"))
            throw new ConflictException("'* → *' cannot be allowed: declare each flow between groups, or between '*' and a specific group.");
        var changes = new List<string>();
        foreach (var group in matrix.Keys.Union(doc.Matrix.Keys).Order(StringComparer.Ordinal))
        {
            var before = doc.Matrix.GetValueOrDefault(group) ?? [];
            var after = matrix.GetValueOrDefault(group) ?? [];
            foreach (var t in after.Except(before)) changes.Add($"{group} → {t} allowed");
            foreach (var t in before.Except(after)) changes.Add($"{group} → {t} revoked");
        }
        doc.Matrix.Clear();
        foreach (var (group, targets) in matrix)
        {
            var distinct = targets.Distinct().Order(StringComparer.Ordinal).ToList();
            if (group != "*" || distinct.Count > 0) doc.Matrix[group] = distinct;
        }
        return changes.Count > 0 ? "Flow matrix: " + string.Join(", ", changes) : "Flow matrix saved, unchanged";
    });

    /// <summary>
    /// Allows each of <paramref name="edges"/> beside what the matrix allows already, and revokes nothing: two administrators who allow
    /// edges at once both keep theirs, where two whole matrices sent at once would leave the second.
    /// </summary>
    public void AllowEdges(IReadOnlyList<GroupEdge> edges, string? by = null) => Edit(by, doc => "Flow matrix: " + Allow(doc, edges));

    /// <summary>Adds to the matrix of <paramref name="doc"/> each edge it does not allow yet, and says which it added.</summary>
    private static string Allow(RightsDocument doc, IReadOnlyList<GroupEdge> edges)
    {
        if (edges.Count == 0) throw new ConflictException("No edge to allow: an edge names the group that sends and the group that receives.");
        var added = new List<string>();
        foreach (var edge in edges)
        {
            var (from, to) = (edge.From?.Trim() ?? "", edge.To?.Trim() ?? "");
            if (from.Length == 0 || to.Length == 0) throw new ConflictException("An edge names the group that sends and the group that receives.");
            if (from == "*" && to == "*")
                throw new ConflictException("'* → *' cannot be allowed: declare each flow between groups, or between '*' and a specific group.");
            if (Allowed(doc, from, to)) continue;
            if (!doc.Matrix.TryGetValue(from, out var targets)) doc.Matrix[from] = targets = [];
            targets.Add(to);
            targets.Sort(StringComparer.Ordinal);
            added.Add($"{from} → {to} allowed");
        }
        return added.Count > 0 ? string.Join(", ", added) : "every edge asked was allowed already";
    }

    /// <summary>Adds <paramref name="identity"/>, and allows <paramref name="edges"/> in the same change when there are any.</summary>
    public void AddIdentity(Identity identity, string? by = null, IReadOnlyList<GroupEdge>? edges = null) => Edit(by, doc =>
    {
        if (doc.Identities.Any(i => i.ClientId == identity.ClientId)) throw new ConflictException($"'{identity.ClientId}' already exists.");
        var added = Normalized(identity);
        CheckUnique([.. doc.Identities, added]);
        doc.Identities.Add(added);
        return edges is { Count: > 0 } ? $"{Added(added)}; flow matrix: {Allow(doc, edges)}" : Added(added);
    });

    private static string Added(Identity added)
    {
        var verified = added.Auth.Mode == AuthModes.Key ? ", verified by a key" : "";
        if (added.IsPerson)
            return $"Identity {added.ClientId} added (person {added.Name}{(added.Account is { } account ? $" ({account})" : "")}, {added.Role}{verified})";
        return added.IsNode
            ? $"Identity {added.ClientId} added (node {added.Name}, groups {string.Join(", ", added.Groups)}{verified})"
            : added.Kind == IdentityKinds.Launcher
                ? $"Identity {added.ClientId} added ({added.Kind} {added.Name}, pipelines {string.Join(", ", added.Pipelines!)}{verified})"
                : $"Identity {added.ClientId} added ({added.Kind} {added.Name}{verified})";
    }

    /// <summary>
    /// Adds what <paramref name="bootstrap"/> names and the rights lack, in one change whose author is <see cref="RightsBootstrap.Author"/>,
    /// and says what it added; a start applies it before anything is served. The administrator is added only while the rights hold no
    /// enabled administrator with a live key; an identity of its id is never replaced: it stops the start unless it is a person whose role
    /// is admin, and is then left as it is, which the answer names. Each other identity is added when its id is absent, and never changed
    /// afterwards. Nothing is written when nothing is added.
    /// </summary>
    public (IReadOnlyList<string> Added, string? AdminLeft) Bootstrap(RightsBootstrap bootstrap)
    {
        if (bootstrap.IsEmpty) return ([], null);
        var added = new List<string>();
        string? left = null;
        string? Additions(RightsDocument doc, DateTimeOffset now)
        {
            added.Clear();
            left = null;
            var wanted = new List<BootstrapIdentity>();
            if (bootstrap.Admin is { } admin && !doc.Identities.Any(i => i is { Enabled: true, IsPerson: true, Role: CoordinatorRoles.Admin } && i.Auth.Mode == AuthModes.Key && i.Auth.LiveAt(now).Any()))
            {
                if (doc.Identities.FirstOrDefault(i => i.ClientId == admin.ClientId) is not { } held) wanted.Add(admin);
                else if (held is not { IsPerson: true, Role: CoordinatorRoles.Admin })
                    throw new InvalidOperationException($"Coordinator:Bootstrap:AdminKey names '{admin.ClientId}', which the rights hold as a {held.Kind}{(held.Role is { } role ? $" ({role})" : "")}: the bootstrap replaces no identity, and the rights hold no administrator with a live key. Give a key of another id.");
                else left = held.ClientId;
            }
            wanted.AddRange(bootstrap.Identities.Where(b => doc.Identities.All(i => i.ClientId != b.ClientId)));
            var changes = new List<string>();
            foreach (var identity in wanted)
            {
                var normalized = Normalized(identity.ToIdentity(now));
                CheckUnique([.. doc.Identities, normalized]);
                doc.Identities.Add(normalized);
                added.Add(normalized.ClientId);
                changes.Add(Added(normalized));
            }
            return changes.Count > 0 ? string.Join("; ", changes) : null;
        }
        // Read first, so that a start which has nothing to add writes nothing.
        if (Additions(Snapshot(), DateTimeOffset.UtcNow) is null) return ([], left);
        try
        {
            Edit(RightsBootstrap.Author, doc => Additions(doc, DateTimeOffset.UtcNow)!);
        }
        catch (ConflictException ex)
        {
            throw new InvalidOperationException($"Coordinator:Bootstrap: {ex.Message}", ex);
        }
        return (added, left);
    }

    /// <summary>
    /// The administrators the rights hold at <paramref name="now"/>: enabled people whose role is admin, verified by the identity provider or
    /// by a key of which one is live.
    /// </summary>
    private static List<string> Administrators(RightsDocument doc, DateTimeOffset now) =>
        [.. doc.Identities.Where(i => i is { Enabled: true, IsPerson: true, Role: CoordinatorRoles.Admin } && (i.Auth.Mode != AuthModes.Key || i.Auth.LiveAt(now).Any()))
            .Select(i => i.ClientId)];

    /// <summary>Whether <c>Coordinator:Roles:Admin</c> lists anyone while a provider can sign them in: an administrator outside the rights.</summary>
    private bool ListsAnAdministrator() =>
        _options.HasProvider && _configuration is not null && CoordinatorRoles.Listed(_configuration, "Admin").Count > 0;

    /// <summary>
    /// Edits an identity. An argument that is null is left as the identity has it: an edit says what it changes, and a field it
    /// leaves out is neither emptied, renamed nor enabled by the omission. The identity is read and written as one change.
    /// </summary>
    public void UpdateIdentity(string clientId, string? name, string? description, IReadOnlyList<string>? groups, bool? enabled, string? by = null,
        IReadOnlyList<string>? pipelines = null, string? role = null, string? account = null) =>
        Edit(by, doc =>
        {
            var index = doc.Identities.FindIndex(i => i.ClientId == clientId);
            if (index < 0) throw new ConflictException($"'{clientId}' does not exist.");
            var before = doc.Identities[index];
            var after = Normalized(before with
            {
                Name = name ?? before.Name,
                Description = description ?? before.Description,
                Groups = groups ?? before.Groups,
                Enabled = enabled ?? before.Enabled,
                Pipelines = pipelines ?? before.Pipelines,
                Role = role ?? before.Role,
                Account = account ?? before.Account,
            });
            CheckUnique(doc.Identities.Select((i, at) => at == index ? after : i).ToList());
            doc.Identities[index] = after;
            var changes = new List<string>();
            if (!before.Groups.SequenceEqual(after.Groups)) changes.Add($"groups {string.Join("+", before.Groups)} → {string.Join("+", after.Groups)}");
            if (!(before.Pipelines ?? []).SequenceEqual(after.Pipelines!)) changes.Add($"pipelines {string.Join("+", before.Pipelines ?? [])} → {string.Join("+", after.Pipelines!)}");
            if (before.Role != after.Role) changes.Add($"role {before.Role} → {after.Role}");
            if (before.Account != after.Account) changes.Add($"account {before.Account} → {after.Account}");
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

    /// <summary>
    /// Makes a key for the identity <paramref name="clientId"/> and keeps its hash: the key itself is in the answer and nowhere else.
    /// Refused for an identity the IdP verifies, for an expiry already past, and while the identity has two live keys: a rotation keeps
    /// the old key live beside the new one until the old one is revoked, never a third.
    /// </summary>
    public (ClearKey Key, IdentityKey Record) AddKey(string clientId, string? label, DateTimeOffset? expires, string? by = null)
    {
        var key = ClearKey.New(clientId);
        IdentityKey? made = null;
        Edit(by, doc =>
        {
            var index = IndexOf(doc, clientId);
            var identity = doc.Identities[index];
            var now = DateTimeOffset.UtcNow;
            if (identity.Auth.Mode != AuthModes.Key)
                throw new ConflictException($"'{clientId}' is verified by the identity provider, so it has no key: an identity's verification is chosen when it is created.");
            if (expires <= now) throw new ConflictException($"A key that expires at {expires:O} has expired already.");
            var live = identity.Auth.LiveAt(now).ToList();
            if (live.Count >= MaxLiveKeys)
                throw new ConflictException($"'{clientId}' has {live.Count} live keys ({string.Join(", ", live.Select(k => k.Id))}): revoke the old one before another rotation.");
            var id = identity.Auth.Keys.Select(k => int.TryParse(k.Id, out var n) ? n : 0).DefaultIfEmpty(0).Max() + 1;
            made = new IdentityKey(id.ToString(System.Globalization.CultureInfo.InvariantCulture), key.Hash, key.Hint, now,
                string.IsNullOrWhiteSpace(label) ? null : label.Trim(), expires);
            doc.Identities[index] = identity with { Auth = identity.Auth with { Keys = [.. identity.Auth.Keys, made] } };
            return $"Identity {clientId}: key {made.Id} made" + (live.Count > 0 ? $", key {live[0].Id} still live (rotation)" : "");
        });
        return (key, made!);
    }

    /// <summary>Revokes key <paramref name="keyId"/> of <paramref name="clientId"/>: it stays in the rights with its date and opens nothing.</summary>
    public void RevokeKey(string clientId, string keyId, string? by = null) => Edit(by, doc =>
    {
        var index = IndexOf(doc, clientId);
        var identity = doc.Identities[index];
        var key = identity.Auth.Keys.FirstOrDefault(k => k.Id == keyId) ?? throw new ConflictException($"'{clientId}' has no key {keyId}.");
        if (key.Revoked is not null) throw new ConflictException($"Key {keyId} of '{clientId}' is revoked already.");
        var now = DateTimeOffset.UtcNow;
        doc.Identities[index] = identity with
        {
            Auth = identity.Auth with { Keys = [.. identity.Auth.Keys.Select(k => k.Id == keyId ? k with { Revoked = now } : k)] },
        };
        return $"Identity {clientId}: key {keyId} revoked";
    });

    /// <summary>
    /// The enabled identity <paramref name="presented"/> opens at <paramref name="now"/>, or null whatever the reason: no identity of the
    /// client id the key names, one the IdP verifies, a disabled one, or a key none of its live keys matches. The client id only finds
    /// the identity; every live key is compared, in constant time, so the answer takes as long whichever matched.
    /// </summary>
    public Identity? KeyHolder(string? presented, DateTimeOffset now) => KeyOf(presented, now)?.Identity;

    /// <summary><see cref="KeyHolder"/>, with the record of the key that matched, which a person's session and token name.</summary>
    public (Identity Identity, IdentityKey Key)? KeyOf(string? presented, DateTimeOffset now)
    {
        if (ClearKey.ClientIdOf(presented) is not { } clientId || Find(clientId) is not { Enabled: true } identity || identity.Auth.Mode != AuthModes.Key)
            return null;
        IdentityKey? matched = null;
        foreach (var key in identity.Auth.LiveAt(now))
            if (ClearKey.Matches(presented!, key.Hash)) matched ??= key;
        return matched is null ? null : (identity, matched);
    }

    /// <summary>
    /// The enabled person verified by a key that <paramref name="clientId"/> names, while its key <paramref name="keyId"/> is live, or
    /// null. A person's session and token are judged by it on every request, so a revoked key, a disabled or removed identity, or a
    /// role taken away applies at the next one.
    /// </summary>
    public Identity? SignedInPerson(string? clientId, string? keyId, DateTimeOffset now) =>
        Find(clientId) is { Enabled: true, IsPerson: true } identity && identity.Auth.Mode == AuthModes.Key && identity.Auth.LiveAt(now).Any(k => k.Id == keyId)
            ? identity
            : null;

    /// <summary>The enabled person of the identity provider whose <see cref="Identity.Account"/> is <paramref name="account"/>, compared without case, or null.</summary>
    public Identity? ProviderPerson(string? account) =>
        string.IsNullOrWhiteSpace(account)
            ? null
            : _state.Doc.Identities.FirstOrDefault(i => i is { Enabled: true, IsPerson: true } && i.Auth.Mode == AuthModes.Idp
                && string.Equals(i.Account, account.Trim(), StringComparison.OrdinalIgnoreCase));

    private static int IndexOf(RightsDocument doc, string clientId)
    {
        var index = doc.Identities.FindIndex(i => i.ClientId == clientId);
        return index >= 0 ? index : throw new ConflictException($"'{clientId}' does not exist.");
    }

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
        else if (groups.Contains("*")) throw new ConflictException($"'{identity.ClientId}' cannot belong to '*': '*' is the flow matrix wildcard, not a group.");
        // Only a launcher lists pipelines: a monitor reads them all, and a list on any other kind would grant nothing.
        var pipelines = (identity.Pipelines ?? []).Select(p => p.Trim()).Where(p => p.Length > 0).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        if (kind != IdentityKinds.Launcher) pipelines = [];
        else if (pipelines.FirstOrDefault(p => !PipelineLibrary.IsValidId(p)) is { } bad)
            throw new ConflictException($"'{bad}' is not a pipeline id: lowercase letters, digits and '-'.");
        // An identity that names no verification is verified by the IdP, and holds no key.
        var auth = identity.Auth ?? IdentityAuth.Idp;
        var mode = string.IsNullOrWhiteSpace(auth.Mode) ? AuthModes.Idp : auth.Mode.Trim();
        if (!AuthModes.All.Contains(mode))
            throw new ConflictException($"'{identity.ClientId}' is verified by '{mode}': an identity is verified by {string.Join(" or ", AuthModes.All)}.");
        if (mode == AuthModes.Key && !IdentityKinds.Keyed.Contains(kind))
            throw new ConflictException($"'{identity.ClientId}' is a {kind}: a {kind} is verified by the identity provider, never by a key.");
        // A person of the provider is found by the account the provider gives it; one verified by a key, by its id.
        var account = identity.Account?.Trim();
        if (kind != IdentityKinds.Person || mode == AuthModes.Key) account = null;
        else if (string.IsNullOrEmpty(account))
            throw new ConflictException($"'{identity.ClientId}' is a person of the identity provider with no account: it is found by the value the provider gives it for Coordinator:Roles:Claim (an email by default).");
        var role = identity.Role?.Trim();
        if (kind != IdentityKinds.Person) role = null;
        else if (role is null || !CoordinatorRoles.All.Contains(role))
            throw new ConflictException($"'{identity.ClientId}' is a person whose role is '{role}': a person is {string.Join(", ", CoordinatorRoles.All)}.");
        var keys = auth.Keys ?? [];
        if (mode == AuthModes.Idp && keys.Count > 0)
            throw new ConflictException($"'{identity.ClientId}' is verified by the identity provider and holds keys: such an identity has none.");
        return identity with
        {
            Name = name,
            Description = identity.Description ?? "",
            Groups = groups,
            Kind = kind,
            Pipelines = pipelines,
            Auth = new IdentityAuth(mode, keys),
            Role = role,
            Account = account,
        };
    }

    /// <summary>
    /// Refuses two identities of one name or one client id, whatever their kinds: an id is what a run, an arming and a rights change
    /// name their author by when it has no email, so it must name one identity.
    /// </summary>
    private static void CheckUnique(IReadOnlyList<Identity> identities)
    {
        foreach (var duplicate in identities.GroupBy(i => i.Name, StringComparer.Ordinal).Where(g => g.Count() > 1))
            throw new ConflictException($"'{duplicate.Key}' is the node name of {string.Join(" and ", duplicate.Select(i => i.ClientId))}: a node name belongs to one identity.");
        foreach (var duplicate in identities.GroupBy(i => i.ClientId, StringComparer.Ordinal).Where(g => g.Count() > 1))
            throw new ConflictException($"'{duplicate.Key}' is the client id of more than one identity.");
        foreach (var duplicate in identities.Where(i => i.Account is not null).GroupBy(i => i.Account!, StringComparer.OrdinalIgnoreCase).Where(g => g.Count() > 1))
            throw new ConflictException($"'{duplicate.Key}' is the account of {string.Join(" and ", duplicate.Select(i => i.ClientId))}: an account names one person.");
    }

    /// <summary>
    /// Applies <paramref name="edit"/>, which changes a copy of the rights and says what it changed, then writes the copy and publishes it.
    /// An edit that is refused, or a store that cannot keep the copy, leaves the rights as they are. Whatever the route, a change that
    /// would leave no administrator where there was one is refused, naming who was: revoking the last live key of the last one,
    /// disabling, removing or demoting them. <c>Coordinator:Roles:Admin</c> counts beside the rights while a provider signs people in.
    /// </summary>
    private void Edit(string? by, Func<RightsDocument, string> edit)
    {
        lock (_write)
        {
            var current = _state;
            var next = Copy(current.Doc);
            var change = edit(next);
            var now = DateTimeOffset.UtcNow;
            if (Administrators(current.Doc, now) is { Count: > 0 } before && Administrators(next, now).Count == 0 && !ListsAnAdministrator())
                throw new ConflictException(
                    $"{string.Join(" and ", before.Select(a => $"'{a}'"))} {(before.Count == 1 ? "is the last administrator" : "are the last administrators")} of this coordinator, and this change would leave none: make another administrator first.");
            next = Record(next, change, by);
            if (_store.Replace(Serialize(next), current.Version) is not { } version)
            {
                Reread();
                throw new ConflictException(
                    "The rights changed in the store since this coordinator read them (another writer, or an earlier change that was kept after it reported failing). They have been read again: check them, and repeat the change if it is not there.");
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
