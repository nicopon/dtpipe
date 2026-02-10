namespace DtPipe.Core.Security;

/// <summary>
/// Helper to sanitize sensitive strings (connection strings) in logs.
/// </summary>
public static class ConnectionStringSanitizer
{
	private const string KeyringPrefix = "keyring://";

	/// <summary>
	/// Returns a safe version of the string for logging.
	/// - If it starts with "keyring://", returns the full alias.
	/// - Otherwise, truncates to 10 chars.
	/// </summary>
	public static string Sanitize(string? input)
	{
		if (string.IsNullOrWhiteSpace(input)) return string.Empty;

		if (input.StartsWith(KeyringPrefix, StringComparison.OrdinalIgnoreCase))
		{
			return input;
		}

		if (input.Length <= 10)
		{
			return input;
		}

		return input.Substring(0, 10) + "...";
	}
}
