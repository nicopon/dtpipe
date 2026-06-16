using System.Collections.Generic;

namespace DtPipe.Cli.Security;

public interface ISecretsManager
{
	void SetSecret(string alias, string value);
	string? GetSecret(string alias);
	void DeleteSecret(string alias);
	Dictionary<string, string> ListSecrets();
	void Nuke();
}
