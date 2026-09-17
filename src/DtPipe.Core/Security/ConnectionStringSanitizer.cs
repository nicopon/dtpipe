using System;
using DtPipe.Core.Abstractions;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace DtPipe.Core.Security;

/// <summary>
/// Removes credentials from strings before they are displayed, logged or handed to a model.
///
/// <para>
/// Two entry points, and the difference between them is how much structure the caller knows.
/// <see cref="Redact"/> receives a connection string — a grammar it can parse — so it shows only
/// keys it recognises as harmless and masks everything else. <see cref="Sanitize"/> receives text
/// with no grammar at all (a driver's exception message, a YAML excerpt, a tool argument), so it
/// can only scan for keys that look sensitive. A scan is best-effort by construction: prefer
/// <see cref="Redact"/> wherever the value is known to be a connection string.
/// </para>
/// </summary>
public static class ConnectionStringSanitizer
{
	private const string KeyringPrefix = "keyring://";

	/// <summary>
	/// Credentials inside a URI: <c>scheme://user:password@host</c>. Applies to both entry points —
	/// a connection string may embed one, and prose may quote one.
	/// </summary>
	private static readonly Regex UriCredentialsRegex = new(
		@"(?<prefix>[a-zA-Z0-9+.-]+://[^:/@\s]+:)(?<password>[^@\s]+)(?<suffix>@)",
		RegexOptions.IgnoreCase | RegexOptions.Compiled);

	/// <summary>
	/// A <c>key=value</c> pair anywhere in free text. The key is captured greedily — the sensitive
	/// test below runs on its normalised form, so a key buried in a larger token is still seen.
	/// </summary>
	private static readonly Regex LooseKeyValueRegex = new(
		@"(?<key>[A-Za-z0-9_.\- ]*[A-Za-z0-9_.\-]\s*=\s*)(?<value>[^;]+)",
		RegexOptions.Compiled);

	/// <summary>An ADO.NET key, once any selector in front of it has been stepped over.</summary>
	private static readonly Regex AdoKeyRegex = new(
		@"^[A-Za-z0-9_.\- ]+$",
		RegexOptions.Compiled);

	/// <summary>
	/// Keys whose value carries no credential, so <see cref="Redact"/> prints them. Normalised
	/// form: lower case, with spaces, underscores, hyphens and dots removed, so one entry covers
	/// <c>User Id</c>, <c>user_id</c> and <c>UserID</c>.
	///
	/// <para>
	/// The list is a parameter of the rule, not a set of special cases: a key missing from it is
	/// masked, which costs a less specific diagnostic and never a leak. Adding a driver's
	/// vocabulary is therefore always safe, and forgetting to is never unsafe.
	/// </para>
	/// </summary>
	private static readonly HashSet<string> SafeKeys = new(StringComparer.Ordinal)
	{
		// Address of the target
		"server", "host", "hostname", "datasource", "address", "networkaddress", "endpoint",
		"blobendpoint", "port", "region", "accountname", "container", "bucket", "path", "file",
		// What is being addressed on it
		"database", "db", "catalog", "initialcatalog", "schema", "table", "format", "compression",
		// Identity, which names the account without authenticating it
		"userid", "uid", "username", "user", "login", "accesskeyid", "keyid",
		// Transport and session settings
		"provider", "driver", "protocol", "defaultendpointsprotocol", "urlstyle", "mode", "cache",
		"version", "readonly", "pooling", "minpoolsize", "maxpoolsize", "timeout", "connecttimeout",
		"connectiontimeout", "commandtimeout", "charset", "encoding", "sslmode", "ssl", "encrypt",
		"trustservercertificate", "integratedsecurity", "applicationname", "applicationintent",
		"localinfile", "allowloadlocalinfile", "allowpublickeyretrieval", "multipleactiveresultsets",
	};

	/// <summary>
	/// Words that make a key sensitive to <see cref="Sanitize"/>, tested as substrings of the
	/// normalised key.
	///
	/// <para>
	/// Substring rather than word boundary, because <c>\b</c> does not break on <c>_</c> nor before
	/// a capital: it finds no boundary in <c>s3_secret_access_key</c> or <c>SecretAccessKey</c>, and
	/// a maintainer who restores it reopens both — along with <c>AccountKey</c> and
	/// <c>SharedAccessSignature</c>, four forms this product builds itself.
	/// </para>
	/// </summary>
	private static readonly string[] SensitiveFragments =
	{
		"password", "pwd", "passwd", "passphrase", "secret", "token", "credential",
		"accountkey", "accesskey", "apikey", "authkey", "privatekey", "signature", "sharedaccess",
	};

	/// <summary>Keys that are sensitive on their own but too short to match as substrings.</summary>
	private static readonly HashSet<string> SensitiveKeys = new(StringComparer.Ordinal)
	{
		"pass", "key", "auth", "sas", "sig", "authorization",
	};

	/// <summary>
	/// Redacts a connection string: every <c>key=value</c> pair whose key is not known harmless is
	/// masked. Fail-closed — an unrecognised key is masked rather than shown.
	/// </summary>
	public static string Redact(string? input)
	{
		if (string.IsNullOrWhiteSpace(input)) return string.Empty;
		if (input.StartsWith(KeyringPrefix, StringComparison.OrdinalIgnoreCase)) return input;

		var text = UriCredentialsRegex.Replace(input, "${prefix}***${suffix}");

		var sb = new StringBuilder(text.Length);
		var segments = text.Split(';');
		for (var i = 0; i < segments.Length; i++)
		{
			if (i > 0) sb.Append(';');
			sb.Append(RedactSegment(segments[i]));
		}
		return sb.ToString();
	}

	/// <summary>
	/// Returns a safe version of free text — a driver's message, a YAML excerpt, a tool argument —
	/// by masking the value of any key that looks sensitive. Best-effort: a credential under a key
	/// this does not recognise survives. Use <see cref="Redact"/> for a connection string.
	/// </summary>
	public static string Sanitize(string? input)
	{
		if (string.IsNullOrWhiteSpace(input)) return string.Empty;
		if (input.StartsWith(KeyringPrefix, StringComparison.OrdinalIgnoreCase)) return input;

		var sanitized = LooseKeyValueRegex.Replace(
			input,
			m => IsSensitive(Normalize(TrailingKey(m.Groups["key"].Value)))
				? m.Groups["key"].Value + "***"
				: m.Value);

		return UriCredentialsRegex.Replace(sanitized, "${prefix}***${suffix}");
	}

	/// <summary>Masks one <c>;</c>-delimited segment, leaving anything that is not a pair alone.</summary>
	private static string RedactSegment(string segment)
	{
		var equals = segment.IndexOf('=');
		if (equals < 0) return segment;

		// A selector in front of the first key must not hide it: without stepping over it,
		// "sqlserver:Password=p" reads as the key "sqlserver:Password", which no rule matches.
		// The grammar — including the (?!//) that keeps a remote URI from reading as a prefix —
		// stays ComponentSelector's. Only the key is read from behind it; the segment is
		// rebuilt from the original text, so the prefix survives verbatim.
		var left = segment.Substring(0, equals);
		var key = ComponentSelector.SkipSelector(left);
		if (!AdoKeyRegex.IsMatch(key)) return segment;

		return SafeKeys.Contains(Normalize(key))
			? segment
			: segment.Substring(0, equals + 1) + "***";
	}

	/// <summary>
	/// The key of a loose match, minus whatever prose preceded it. <c>"SET s3_secret_access_key="</c>
	/// and <c>"the account AccountKey="</c> both keep only the last space-separated token.
	/// </summary>
	private static string TrailingKey(string captured)
	{
		var key = captured.TrimEnd().TrimEnd('=').TrimEnd();
		var space = key.LastIndexOf(' ');
		return space < 0 ? key : key.Substring(space + 1);
	}

	private static bool IsSensitive(string normalizedKey)
	{
		if (normalizedKey.Length == 0) return false;

		// One vocabulary for both entry points. Without this, "AccessKeyId" — a public identifier
		// Redact prints — is masked by Sanitize for containing "accesskey", and the two disagree
		// about the same key.
		if (SafeKeys.Contains(normalizedKey)) return false;
		if (SensitiveKeys.Contains(normalizedKey)) return true;

		foreach (var fragment in SensitiveFragments)
		{
			if (normalizedKey.Contains(fragment, StringComparison.Ordinal)) return true;
		}
		return false;
	}

	/// <summary>Lower case with the separators drivers disagree about removed.</summary>
	private static string Normalize(string key)
	{
		var sb = new StringBuilder(key.Length);
		foreach (var c in key)
		{
			if (c is ' ' or '_' or '-' or '.') continue;
			sb.Append(char.ToLowerInvariant(c));
		}
		return sb.ToString();
	}
}
