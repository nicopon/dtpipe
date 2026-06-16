using System;
using System.Collections.Generic;

namespace DtPipe.Cli.Security;

public class InMemorySecretsManager : ISecretsManager
{
	private readonly Dictionary<string, string> _secrets = new(StringComparer.OrdinalIgnoreCase);

	public void SetSecret(string alias, string value)
	{
		_secrets[alias] = value;
	}

	public string? GetSecret(string alias)
	{
		return _secrets.TryGetValue(alias, out var val) ? val : null;
	}

	public void DeleteSecret(string alias)
	{
		_secrets.Remove(alias);
	}

	public Dictionary<string, string> ListSecrets()
	{
		return new Dictionary<string, string>(_secrets);
	}

	public void Nuke()
	{
		_secrets.Clear();
	}
}
