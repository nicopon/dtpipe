using System.Threading;

namespace DtPipe.Core.Security;

/// <summary>
/// Default implementation of <see cref="IMcpSecurityContext"/> tracking MCP session security context.
/// </summary>
public class McpSecurityContext : IMcpSecurityContext
{
    private static readonly AsyncLocal<bool> _isMcpSession = new();

    /// <summary>
    /// Gets or sets whether the current execution context is an MCP session.
    /// </summary>
    public bool IsMcpSession
    {
        get => _isMcpSession.Value;
        set => _isMcpSession.Value = value;
    }
}
