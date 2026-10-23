namespace DtPipe.Cli.Security;

/// <summary>
/// The context for an approval request — a write/execution attempt that the guardrails (F2) must
/// decide on.
/// </summary>
public sealed class ApprovalRequest
{
    /// <summary>The pipeline YAML being executed.</summary>
    public string Yaml { get; init; } = string.Empty;

    /// <summary>True when the caller asked for a real write (the <c>apply</c> flag).</summary>
    public bool Apply { get; init; }

    /// <summary>True when a real terminal is attached and can prompt the user.</summary>
    public bool Interactive { get; init; }

    /// <summary>A short human-readable summary of what would be written.</summary>
    public string Description { get; init; } = string.Empty;
}

/// <summary>
/// Gate that decides whether an execution may proceed (F2 — approval gate). The default is
/// <b>deny</b>: in a non-interactive context writes are refused (read-only). This makes the
/// agent fail-closed: without explicit approval, no real write happens.
/// </summary>
public interface IApprovalGate
{
    /// <summary>Approve or refuse an execution request.</summary>
    bool Approve(ApprovalRequest request);
}

/// <summary>
/// Default approval gate. A write is approved only when <see cref="ApprovalRequest.Apply"/>
/// is requested AND the context is interactive (a human can confirm). Otherwise — notably
/// in agent/MCP non-interactive runs — writes are refused and only dry-run is permitted.
/// </summary>
public sealed class DefaultApprovalGate : IApprovalGate
{
    /// <summary>
    /// Optional predicate an operator can inject to override the interactive decision
    /// (e.g. an explicit <c>--apply</c> flag without a TTY). Returns true to approve.
    /// Settable so a command can wire its operator consent in after construction.
    /// </summary>
    public Func<ApprovalRequest, bool>? Override { get; set; }

    public DefaultApprovalGate()
          => Override = null;

    public DefaultApprovalGate(Func<ApprovalRequest, bool> overridePredicate)
          => Override = overridePredicate;

    public bool Approve(ApprovalRequest request)
    {
        // The caller must explicitly request a real write.
        if (!request.Apply)
            return false;

        // An injected override (explicit --apply without a TTY) approves.
        if (Override != null && Override(request))
            return true;

        // Otherwise a write requires an interactive human confirmation.
        return request.Interactive;
    }
}
