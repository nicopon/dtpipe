using System;
using System.Text.RegularExpressions;

namespace DtPipe.Core.Security;

/// <summary>
/// Helper to sanitize sensitive strings (connection strings) in logs and console output.
/// </summary>
public static class ConnectionStringSanitizer
{
	private const string KeyringPrefix = "keyring://";

	// Regex to match password/secret/token values in connection strings (key=value)
	private static readonly Regex KeyValueRegex = new(
		@"(?<key>\b(password|pwd|secret|token|credential|pass)\b\s*=\s*)(?<value>[^;]+)",
		RegexOptions.IgnoreCase | RegexOptions.Compiled);

	// Regex to match credentials in URIs: scheme://user:password@host
	private static readonly Regex UriCredentialsRegex = new(
		@"(?<prefix>[a-zA-Z0-9+-.]+://[^:]+:)(?<password>[^@]+)(?<suffix>@)",
		RegexOptions.IgnoreCase | RegexOptions.Compiled);

	/// <summary>
	/// Returns a safe version of the connection string or URI for logging and console display.
	/// </summary>
	public static string Sanitize(string? input)
	{
		if (string.IsNullOrWhiteSpace(input)) return string.Empty;

		// Keep keyring references as is since they don't expose secrets directly
		if (input.StartsWith(KeyringPrefix, StringComparison.OrdinalIgnoreCase))
		{
			return input;
		}

		// 1. Sanitize standard key=value connection strings
		string sanitized = KeyValueRegex.Replace(input, "${key}***");

		// 2. Sanitize URI style connection strings
		sanitized = UriCredentialsRegex.Replace(sanitized, "${prefix}***${suffix}");

		return sanitized;
	}
}
