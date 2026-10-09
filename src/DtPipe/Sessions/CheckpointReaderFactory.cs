using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;

namespace DtPipe.Sessions;

/// <summary>Options placeholder — a checkpoint source carries no adapter configuration.</summary>
public sealed class CheckpointReaderOptions : IOptionSet
{
    public static string Prefix => "checkpoint";
    public static string DisplayName => "Checkpoint Source";
}

/// <summary>
/// Makes a stored checkpoint a pipeline source.
///
/// It is selected by capability, from --from-checkpoint, and never through
/// <c>ComponentSelector</c>: a checkpoint key is a hex string, not a connection string, and
/// letting it through the <c>{component}[+{variant}]:</c> grammar is how a key would one day be
/// read as a prefix. The router hands this factory over directly, the same way it does for a
/// typed channel endpoint.
/// </summary>
public sealed class CheckpointReaderFactory : IStreamReaderFactory
{
    private readonly string _checkpointKey;
    private readonly string? _sessionName;

    public CheckpointReaderFactory(string checkpointKey, string? sessionName)
    {
        _checkpointKey = checkpointKey;
        _sessionName = sessionName;
    }

    public string ComponentName => "checkpoint";
    public string Category => "Session";
    public Type OptionsType => typeof(CheckpointReaderOptions);
    public bool RequiresQuery => false;

    /// <summary>A stored checkpoint holds rows an earlier run already bounded.</summary>
    public bool IsSource => false;

    /// <summary>
    /// Always false. A checkpoint is never reached by inspecting a connection string — the
    /// router selects this factory from the flag, and nothing else may claim the key.
    /// </summary>
    public bool CanHandle(string connectionString) => false;

    public IEnumerable<Type> GetSupportedOptionTypes() => [typeof(CheckpointReaderOptions)];

    public IStreamReader Create(OptionsRegistry registry)
    {
        var session = SessionStore.Resolve(_sessionName);
        var store = new CheckpointStore(session);

        if (!store.Contains(_checkpointKey))
        {
            var available = store.List();
            var hint = available.Count == 0
                ? "That session holds no checkpoints. Materialise one with --checkpoint first."
                : $"Available in this session: {string.Join(", ", available)}.";
            throw new InvalidOperationException(
                $"No checkpoint '{_checkpointKey}' in session '{session.Identity.Name}'. {hint}");
        }

        return new CheckpointStreamReader(store, _checkpointKey);
    }
}
