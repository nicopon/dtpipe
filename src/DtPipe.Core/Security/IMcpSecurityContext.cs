namespace DtPipe.Core.Security;

/// <summary>
/// Service to track if the current execution context is within an MCP (Model Context Protocol) session.
/// Useful for applying tighter security sandboxes (e.g. disabling external DB access in DuckDB).
/// </summary>
public interface IMcpSecurityContext
{
    /// <summary>
    /// Gets or sets whether the current session is an MCP session.
    /// </summary>
    bool IsMcpSession { get; set; }
}
