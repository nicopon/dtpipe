using DtPipe.Cli.Expressions;

namespace DtPipe.Cli.Security;

public sealed class KeyringInterpolator : IHostInterpolator
{
    private readonly ISecretsManager _secretsManager;

    public KeyringInterpolator(ISecretsManager secretsManager)
    {
        _secretsManager = secretsManager;
    }

    public Task<string?> TryResolveAsync(string expression, CancellationToken ct)
    {
        if (expression.StartsWith("keyring://", StringComparison.OrdinalIgnoreCase))
        {
            var keyName = expression["keyring://".Length..];
            return Task.FromResult(_secretsManager.GetSecret(keyName));
        }

        return Task.FromResult<string?>(null);
    }
}
