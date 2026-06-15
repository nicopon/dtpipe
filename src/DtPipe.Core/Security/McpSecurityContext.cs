namespace DtPipe.Core.Security;

/// <summary>
/// Context to track if the current execution is within an MCP (Model Context Protocol) session.
/// Useful for applying tighter security sandboxes.
/// </summary>
public static class McpSecurityContext
{
	/// <summary>
	/// Gets or sets whether the current session is an MCP session.
	/// </summary>
	public static bool IsMcpSession { get; set; }
}
