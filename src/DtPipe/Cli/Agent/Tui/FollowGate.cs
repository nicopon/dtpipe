namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// Whether a live band follows its own tail. Pure — the panel hands it a clock reading rather than
/// taking one, so the whole timing contract is asserted from a scripted sequence without a terminal
/// and without waiting for real seconds to pass.
///
/// <para>
/// Reading a band means stopping it, but only for as long as reading takes. Taking the focus holds
/// the tail; every scroll renews the hold; the hold lapses on its own, so a band left focused does
/// not stay frozen for the rest of the session. Losing the focus resumes at once — the reader has
/// visibly gone elsewhere.
/// </para>
/// </summary>
internal struct FollowGate
{
    /// <summary>How long a hold outlives the last interaction.</summary>
    public const long HoldMs = 4000;

    private long _until;
    private bool _focused;

    /// <summary>The band took the focus: hold the tail, starting the clock.</summary>
    public void Focused(long now)
    {
        _focused = true;
        _until = now + HoldMs;
    }

    /// <summary>The band lost the focus: follow again, immediately.</summary>
    public void Blurred()
    {
        _focused = false;
        _until = 0;
    }

    /// <summary>
    /// The reader moved through the band — a key, a wheel, a drag. Renews the hold. Ignored while
    /// unfocused, so the panel's own re-pinning of the tail cannot renew a hold nobody asked for.
    /// </summary>
    public void Interacted(long now)
    {
        if (_focused) _until = now + HoldMs;
    }

    /// <summary>Whether the band should jump to its newest line.</summary>
    public readonly bool ShouldFollow(long now) => !_focused || now >= _until;
}
